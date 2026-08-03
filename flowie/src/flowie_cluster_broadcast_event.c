#include "flowie_cluster_broadcast_event_internal.h"

#include "flowie_cluster_peer_wire_internal.h"

#include "monocypher.h"

#include <string.h>

static const uint8_t FLOWIE_CLUSTER_BROADCAST_EVENT_MAGIC[4] = {'T', 'F', 'B', 'E'};
static const uint8_t FLOWIE_CLUSTER_BROADCAST_EVENT_DIGEST_DOMAIN[] =
    "flowie.cluster.broadcast.event.v1";
static const uint8_t FLOWIE_CLUSTER_BROADCAST_TARGET_COMMAND_DOMAIN[] =
    "flowie.cluster.broadcast.target.command.v1";

static int flowie_cluster_broadcast_nonzero(const uint8_t *bytes, size_t size) {
  uint8_t combined = 0u;
  if (!bytes) return 0;
  for (size_t slot = 0u; slot < size; ++slot)
    combined |= bytes[slot];
  return combined != 0u;
}

static int flowie_cluster_broadcast_publish_identity_valid(
    const flowie_cluster_pgsql_outbox_event_t *event,
    const flowie_cluster_publish_event_view_t *publish) {
  return event && publish && event->record_kind == FLOWIE_CLUSTER_KEY_SESSION &&
         event->record_key && publish->publish.client_id.data &&
         tstr_len(event->record_key) == publish->publish.client_id.size &&
         memcmp(event->record_key, publish->publish.client_id.data,
                publish->publish.client_id.size) == 0;
}

int flowie_cluster_broadcast_event_encode(const flowie_cluster_pgsql_outbox_event_t *event,
                                          size_t max_payload_size, tstr_t *out) {
  flowie_cluster_publish_event_view_t publish = FLOWIE_CLUSTER_PUBLISH_EVENT_VIEW_INIT;
  size_t payload_size;
  size_t total_size;
  uint8_t *encoded;
  int rc;
  if (!out) return TURBO_EINVAL;
  *out = NULL;
  if (!event || event->size < sizeof(*event) ||
      event->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      !flowie_cluster_broadcast_nonzero(event->command_id, sizeof(event->command_id)) ||
      event->event_owner_epoch == 0u ||
      event->event_type != FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE || !event->payload)
    return TURBO_EINVAL;
  payload_size = tstr_len(event->payload);
  if (payload_size > SIZE_MAX - FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE)
    return TURBO_ERANGE;
  total_size = FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE + payload_size;
  if (total_size > max_payload_size || total_size > UINT32_MAX || payload_size > UINT32_MAX)
    return TURBO_EMSGSIZE;
  rc = flowie_cluster_publish_event_decode(event->payload, payload_size, payload_size, &publish);
  if (rc != TURBO_OK) return rc;
  if (!flowie_cluster_broadcast_publish_identity_valid(event, &publish)) return TURBO_EPROTO;
  *out = tstr_new_len(NULL, total_size);
  if (!*out) return TURBO_ENOMEM;
  encoded = (uint8_t *)*out;
  memset(encoded, 0, FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE);
  memcpy(encoded, FLOWIE_CLUSTER_BROADCAST_EVENT_MAGIC,
         sizeof(FLOWIE_CLUSTER_BROADCAST_EVENT_MAGIC));
  flowie_cluster_peer_wire_write_u16(encoded + 4u, FLOWIE_CLUSTER_BROADCAST_EVENT_VERSION);
  flowie_cluster_peer_wire_write_u16(encoded + 6u, FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE);
  flowie_cluster_peer_wire_write_u32(encoded + 8u, (uint32_t)total_size);
  flowie_cluster_peer_wire_write_u32(encoded + 12u, (uint32_t)payload_size);
  memcpy(encoded + 16u, event->command_id, FLOWIE_CLUSTER_COMMAND_ID_SIZE);
  flowie_cluster_peer_wire_write_u32(encoded + 32u, event->event_index);
  flowie_cluster_peer_wire_write_u32(encoded + 36u, event->shard_id);
  flowie_cluster_peer_wire_write_u64(encoded + 40u, event->event_owner_epoch);
  flowie_cluster_peer_wire_write_u64(encoded + 48u, event->fact_revision);
  flowie_cluster_peer_wire_write_u32(encoded + 56u, (uint32_t)event->event_type);
  memcpy(encoded + FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE, event->payload, payload_size);
  return TURBO_OK;
}

int flowie_cluster_broadcast_event_decode(const void *data, size_t data_size,
                                          size_t max_payload_size,
                                          flowie_cluster_broadcast_event_view_t *out) {
  const uint8_t *bytes = (const uint8_t *)data;
  flowie_cluster_broadcast_event_view_t decoded = FLOWIE_CLUSTER_BROADCAST_EVENT_VIEW_INIT;
  uint32_t payload_size;
  uint32_t event_type;
  int rc;
  if (!bytes || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_BROADCAST_EVENT_VERSION ||
      max_payload_size <= FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE)
    return TURBO_EINVAL;
  if (data_size > max_payload_size) return TURBO_EMSGSIZE;
  if (data_size <= FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
      memcmp(bytes, FLOWIE_CLUSTER_BROADCAST_EVENT_MAGIC,
             sizeof(FLOWIE_CLUSTER_BROADCAST_EVENT_MAGIC)) != 0 ||
      flowie_cluster_peer_wire_read_u16(bytes + 4u) != FLOWIE_CLUSTER_BROADCAST_EVENT_VERSION ||
      flowie_cluster_peer_wire_read_u16(bytes + 6u) !=
          FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
      flowie_cluster_peer_wire_read_u32(bytes + 8u) != data_size)
    return TURBO_EPROTO;
  payload_size = flowie_cluster_peer_wire_read_u32(bytes + 12u);
  event_type = flowie_cluster_peer_wire_read_u32(bytes + 56u);
  if (payload_size != data_size - FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE ||
      !flowie_cluster_broadcast_nonzero(bytes + 16u, FLOWIE_CLUSTER_COMMAND_ID_SIZE) ||
      flowie_cluster_peer_wire_read_u64(bytes + 40u) == 0u ||
      event_type != FLOWIE_CLUSTER_PUBLISH_OUTBOX_EVENT_TYPE ||
      flowie_cluster_peer_wire_read_u32(bytes + 60u) != 0u)
    return TURBO_EPROTO;
  rc = flowie_cluster_publish_event_decode(
      bytes + FLOWIE_CLUSTER_BROADCAST_EVENT_HEADER_SIZE, payload_size, payload_size,
      &decoded.publish);
  if (rc != TURBO_OK) return rc;
  memcpy(decoded.command_id, bytes + 16u, sizeof(decoded.command_id));
  decoded.event_index = flowie_cluster_peer_wire_read_u32(bytes + 32u);
  decoded.source_shard_id = flowie_cluster_peer_wire_read_u32(bytes + 36u);
  decoded.source_owner_epoch = flowie_cluster_peer_wire_read_u64(bytes + 40u);
  decoded.fact_revision = flowie_cluster_peer_wire_read_u64(bytes + 48u);
  *out = decoded;
  return TURBO_OK;
}

int flowie_cluster_broadcast_event_digest(
    const void *data, size_t data_size, size_t max_payload_size,
    uint8_t out[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE]) {
  flowie_cluster_broadcast_event_view_t decoded = FLOWIE_CLUSTER_BROADCAST_EVENT_VIEW_INIT;
  crypto_blake2b_ctx ctx;
  int rc;
  if (!out) return TURBO_EINVAL;
  memset(out, 0, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE);
  rc = flowie_cluster_broadcast_event_decode(data, data_size, max_payload_size, &decoded);
  if (rc != TURBO_OK) return rc;
  crypto_blake2b_init(&ctx, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE);
  crypto_blake2b_update(&ctx, FLOWIE_CLUSTER_BROADCAST_EVENT_DIGEST_DOMAIN,
                        sizeof(FLOWIE_CLUSTER_BROADCAST_EVENT_DIGEST_DOMAIN) - 1u);
  crypto_blake2b_update(&ctx, (const uint8_t *)data, data_size);
  crypto_blake2b_final(&ctx, out);
  return TURBO_OK;
}

int flowie_cluster_broadcast_target_command_id(
    const uint8_t event_digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE], uint32_t target_shard_id,
    uint64_t target_session_id, uint8_t out[FLOWIE_CLUSTER_COMMAND_ID_SIZE]) {
  uint8_t identity[12];
  crypto_blake2b_ctx ctx;
  if (out) memset(out, 0, FLOWIE_CLUSTER_COMMAND_ID_SIZE);
  if (!event_digest || !out ||
      !flowie_cluster_broadcast_nonzero(event_digest, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE))
    return TURBO_EINVAL;
  flowie_cluster_peer_wire_write_u32(identity, target_shard_id);
  flowie_cluster_peer_wire_write_u64(identity + 4u, target_session_id);
  crypto_blake2b_init(&ctx, FLOWIE_CLUSTER_COMMAND_ID_SIZE);
  crypto_blake2b_update(&ctx, FLOWIE_CLUSTER_BROADCAST_TARGET_COMMAND_DOMAIN,
                        sizeof(FLOWIE_CLUSTER_BROADCAST_TARGET_COMMAND_DOMAIN) - 1u);
  crypto_blake2b_update(&ctx, event_digest, FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE);
  crypto_blake2b_update(&ctx, identity, sizeof(identity));
  crypto_blake2b_final(&ctx, out);
  crypto_wipe(&ctx, sizeof(ctx));
  crypto_wipe(identity, sizeof(identity));
  return TURBO_OK;
}

int flowie_cluster_broadcast_shard_ack_command(
    const flowie_cluster_owner_token_t *current_owner,
    const flowie_cluster_broadcast_event_view_t *source,
    const uint8_t event_digest[FLOWIE_CLUSTER_COMMAND_DIGEST_SIZE],
    flowie_cluster_pgsql_event_dedupe_t *dedupe,
    flowie_cluster_pgsql_fact_command_t *command) {
  flowie_cluster_pgsql_event_dedupe_t marker = FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
  flowie_cluster_pgsql_fact_command_t built = FLOWIE_CLUSTER_PGSQL_FACT_COMMAND_INIT;
  int rc;
  if (!current_owner ||
      flowie_cluster_owner_token_require(current_owner, current_owner) != TURBO_OK || !source ||
      source->size != sizeof(*source) ||
      source->abi_version != FLOWIE_CLUSTER_BROADCAST_EVENT_VERSION ||
      !flowie_cluster_broadcast_nonzero(source->command_id, sizeof(source->command_id)) ||
      source->source_owner_epoch == 0u || !event_digest || !dedupe || !command)
    return TURBO_EINVAL;
  memcpy(marker.source_command_id, source->command_id, sizeof(marker.source_command_id));
  marker.event_index = source->event_index;
  marker.source_shard_id = source->source_shard_id;
  marker.source_owner_epoch = source->source_owner_epoch;
  marker.source_fact_revision = source->fact_revision;
  memcpy(marker.event_digest, event_digest, sizeof(marker.event_digest));
  rc = flowie_cluster_broadcast_target_command_id(event_digest, current_owner->shard_id, 0u,
                                                  built.command_id);
  if (rc != TURBO_OK) return rc;
  built.owner = *current_owner;
  *dedupe = marker;
  built.dedupe = dedupe;
  *command = built;
  return TURBO_OK;
}
