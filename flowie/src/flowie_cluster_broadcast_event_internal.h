#ifndef FLOWIE_CLUSTER_BROADCAST_EVENT_INTERNAL_H
#define FLOWIE_CLUSTER_BROADCAST_EVENT_INTERNAL_H

#include "flowie_cluster_pgsql_internal.h"
#include "flowie_cluster_publish_event_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_BROADCAST_EVENT_VERSION 1u
#define FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE 64u

/**
 * Stable source identity plus a borrowed TFPE payload. Redis stream IDs and
 * consumer offsets are deliberately absent: they are transport metadata, not
 * MQTT dedupe facts.
 */
typedef struct flowie_cluster_broadcast_event_view_s {
  size_t size;
  uint32_t abi_version;
  uint8_t command_id[FLOWIE_CLUSTER_COMMAND_ID_SIZE];
  uint32_t event_index;
  uint32_t source_shard_id;
  uint64_t source_owner_epoch;
  uint64_t fact_revision;
  flowie_cluster_publish_event_view_t publish;
} flowie_cluster_broadcast_event_view_t;

#define FLOWIE_CLUSTER_BROADCAST_EVENT_VIEW_INIT                                                  \
  {sizeof(flowie_cluster_broadcast_event_view_t), FLOWIE_CLUSTER_BROADCAST_EVENT_VERSION, {0},   \
   0u, 0u, 0u, 0u, FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT}

/** Encode one validated PostgreSQL publish outbox row for the broadcast bus. */
int flowie_cluster_broadcast_event_encode(const flowie_cluster_pgsql_outbox_event_t *event,
                                          size_t max_payload_size, tstr_t *out);

/** Decode and validate a transport-independent TFBE broadcast envelope. */
int flowie_cluster_broadcast_event_decode(const void *data, size_t data_size,
                                          size_t max_payload_size,
                                          flowie_cluster_broadcast_event_view_t *out);

/**
 * Validate and hash the complete immutable TFBE envelope for PostgreSQL target
 * dedupe. Transport metadata such as a Redis stream ID must not be included.
 */
int flowie_cluster_broadcast_event_digest(
    const void *data, size_t data_size, size_t max_payload_size,
    uint8_t out[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE]);

/** Derive one stable PostgreSQL target command ID from immutable event identity. */
int flowie_cluster_broadcast_target_command_id(
    const uint8_t event_digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE], uint32_t target_shard_id,
    uint64_t target_session_id, uint8_t out[FLOWIE_CLUSTER_COMMAND_ID_SIZE]);

/** Build the zero-mutation PostgreSQL marker that completes one target shard. */
int flowie_cluster_broadcast_shard_ack_command(
    const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_broadcast_event_view_t *source,
    const uint8_t event_digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE],
    flowie_cluster_pgsql_event_dedupe_t *dedupe,
    flowie_cluster_pgsql_fact_command_t *command);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_BROADCAST_EVENT_INTERNAL_H */
