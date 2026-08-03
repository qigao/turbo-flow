#include "flowie_cluster_internal.h"

#include "turbo_error.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

#define FLOWIE_CLUSTER_HASH_SEED UINT64_C(0x464c4f5749450001)
#define FLOWIE_CLUSTER_NS_PER_MS UINT64_C(1000000)
#define FLOWIE_CLUSTER_MS_PER_SECOND UINT64_C(1000)

struct flowie_cluster_runtime_s {
  flowie_cluster_owner_token_t local_owner;
  uint8_t cluster_id[16];
  size_t cluster_id_size;
  uint8_t listener_id[16];
  size_t listener_id_size;
};

static int flowie_cluster_u64_add(uint64_t left, uint64_t right, uint64_t *out) {
  if (!out) return TURBO_EINVAL;
  if (right > UINT64_MAX - left) return TURBO_ERANGE;
  *out = left + right;
  return TURBO_OK;
}

static int flowie_cluster_u64_mul(uint64_t left, uint64_t right, uint64_t *out) {
  if (!out) return TURBO_EINVAL;
  if (left != 0u && right > UINT64_MAX / left) return TURBO_ERANGE;
  *out = left * right;
  return TURBO_OK;
}

static void flowie_cluster_write_u64_be(uint8_t out[8], uint64_t value) {
  for (size_t i = 0u; i < 8u; ++i)
    out[i] = (uint8_t)(value >> (56u - i * 8u));
}

static int flowie_cluster_node_state_valid(flowie_cluster_node_state_t state) {
  return state >= FLOWIE_CLUSTER_NODE_STARTING && state <= FLOWIE_CLUSTER_NODE_EXPIRED;
}

static int flowie_cluster_shard_state_valid(flowie_cluster_shard_state_t state) {
  return state >= FLOWIE_CLUSTER_SHARD_UNASSIGNED && state <= FLOWIE_CLUSTER_SHARD_FENCED;
}

static int flowie_cluster_connection_state_valid(flowie_cluster_connection_state_t state) {
  return state >= FLOWIE_CLUSTER_CONNECTION_ACCEPTED && state <= FLOWIE_CLUSTER_CONNECTION_FAILED;
}

static int flowie_cluster_boot_id_valid(const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  uint8_t combined = 0u;
  if (!boot_id) return 0;
  for (size_t i = 0u; i < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++i)
    combined |= boot_id[i];
  return combined != 0u;
}

static int flowie_cluster_owner_token_valid(const flowie_cluster_owner_token_t *token) {
  return token && token->size >= sizeof(*token) &&
         token->abi_version == FLOWIE_CLUSTER_INTERNAL_ABI_V1 && token->owner_epoch != 0u &&
         token->node_id_size != 0u && token->node_id_size <= FLOWIE_CLUSTER_NODE_ID_MAX &&
         token->node_id[token->node_id_size] == '\0' &&
         flowie_cluster_boot_id_valid(token->boot_id);
}

static int flowie_cluster_shard_lease_valid(const flowie_cluster_shard_lease_t *lease) {
  if (!lease || lease->size < sizeof(*lease) ||
      lease->abi_version != FLOWIE_CLUSTER_INTERNAL_ABI_V1 || lease->owned > 1u) {
    return 0;
  }
  if (!lease->owned) return lease->lease_until_db_ms == 0u;
  return lease->lease_until_db_ms != 0u && flowie_cluster_owner_token_valid(&lease->owner);
}

int flowie_cluster_config_validate(const flowie_cluster_config_t *config) {
  uint64_t renewal_budget;
  int rc;
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_INTERNAL_ABI_V1 ||
      config->hash_version != FLOWIE_CLUSTER_HASH_VERSION_1 || config->shard_count == 0u ||
      config->shard_count > FLOWIE_CLUSTER_SHARD_COUNT_MAX || config->max_nodes == 0u ||
      config->max_nodes > FLOWIE_CLUSTER_NODE_COUNT_MAX || config->lease_ttl_ms == 0u ||
      config->renew_interval_ms == 0u || config->worst_case_db_latency_ms == 0u ||
      config->safety_margin_ms == 0u || config->peer_queue_entries == 0u ||
      config->peer_queue_bytes == 0u || config->max_command_bytes == 0u ||
      config->max_command_bytes > config->peer_queue_bytes || config->outbox_records == 0u ||
      config->outbox_bytes == 0u) {
    return TURBO_EINVAL;
  }
  rc = flowie_cluster_u64_add(config->renew_interval_ms, config->worst_case_db_latency_ms,
                              &renewal_budget);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_u64_add(renewal_budget, config->safety_margin_ms, &renewal_budget);
  if (rc != TURBO_OK) return rc;
  return renewal_budget < config->lease_ttl_ms ? TURBO_OK : TURBO_EINVAL;
}

int flowie_cluster_shard_for_key(uint32_t hash_version, flowie_cluster_key_kind_t kind,
                                 const uint8_t *cluster_id, size_t cluster_id_size,
                                 const uint8_t *listener_id, size_t listener_id_size,
                                 const uint8_t *key, size_t key_size, uint32_t shard_count,
                                 uint32_t *out_shard) {
  uint8_t tuple[50];
  uint64_t hash;
  if (out_shard) *out_shard = 0u;
  if (hash_version != FLOWIE_CLUSTER_HASH_VERSION_1 ||
      (kind != FLOWIE_CLUSTER_KEY_SESSION && kind != FLOWIE_CLUSTER_KEY_RETAINED) || !cluster_id ||
      cluster_id_size == 0u || cluster_id_size > FLOWIE_CLUSTER_ID_MAX || !listener_id ||
      listener_id_size == 0u || listener_id_size > FLOWIE_CLUSTER_LISTENER_ID_MAX || !key ||
      key_size == 0u || key_size > FLOWIE_CLUSTER_KEY_MAX || shard_count == 0u ||
      shard_count > FLOWIE_CLUSTER_SHARD_COUNT_MAX || !out_shard) {
    return TURBO_EINVAL;
  }

  tuple[0] = (uint8_t)hash_version;
  tuple[1] = (uint8_t)kind;
  flowie_cluster_write_u64_be(tuple + 2u, (uint64_t)cluster_id_size);
  flowie_cluster_write_u64_be(
      tuple + 10u, XXH3_64bits_withSeed(cluster_id, cluster_id_size, FLOWIE_CLUSTER_HASH_SEED));
  flowie_cluster_write_u64_be(tuple + 18u, (uint64_t)listener_id_size);
  flowie_cluster_write_u64_be(
      tuple + 26u, XXH3_64bits_withSeed(listener_id, listener_id_size, FLOWIE_CLUSTER_HASH_SEED));
  flowie_cluster_write_u64_be(tuple + 34u, (uint64_t)key_size);
  flowie_cluster_write_u64_be(tuple + 42u,
                              XXH3_64bits_withSeed(key, key_size, FLOWIE_CLUSTER_HASH_SEED));
  hash = XXH3_64bits_withSeed(tuple, sizeof(tuple), FLOWIE_CLUSTER_HASH_SEED);
  *out_shard = (uint32_t)(hash % shard_count);
  return TURBO_OK;
}

int flowie_cluster_node_transition_validate(flowie_cluster_node_state_t from,
                                            flowie_cluster_node_state_t to) {
  int allowed = 0;
  if (!flowie_cluster_node_state_valid(from) || !flowie_cluster_node_state_valid(to))
    return TURBO_EINVAL;
  if (from == to) return TURBO_EALREADY;
  switch (from) {
  case FLOWIE_CLUSTER_NODE_STARTING:
    allowed = to == FLOWIE_CLUSTER_NODE_SYNCING || to == FLOWIE_CLUSTER_NODE_OFFLINE;
    break;
  case FLOWIE_CLUSTER_NODE_SYNCING:
    allowed = to == FLOWIE_CLUSTER_NODE_READY || to == FLOWIE_CLUSTER_NODE_DRAINING ||
              to == FLOWIE_CLUSTER_NODE_OFFLINE || to == FLOWIE_CLUSTER_NODE_EXPIRED;
    break;
  case FLOWIE_CLUSTER_NODE_READY:
    allowed = to == FLOWIE_CLUSTER_NODE_DRAINING || to == FLOWIE_CLUSTER_NODE_EXPIRED;
    break;
  case FLOWIE_CLUSTER_NODE_DRAINING:
    allowed = to == FLOWIE_CLUSTER_NODE_OFFLINE || to == FLOWIE_CLUSTER_NODE_EXPIRED;
    break;
  case FLOWIE_CLUSTER_NODE_OFFLINE:
  case FLOWIE_CLUSTER_NODE_EXPIRED:
    break;
  }
  return allowed ? TURBO_OK : TURBO_EBUSY;
}

int flowie_cluster_shard_transition_validate(flowie_cluster_shard_state_t from,
                                             flowie_cluster_shard_state_t to) {
  int allowed = 0;
  if (!flowie_cluster_shard_state_valid(from) || !flowie_cluster_shard_state_valid(to))
    return TURBO_EINVAL;
  if (from == to) return TURBO_EALREADY;
  switch (from) {
  case FLOWIE_CLUSTER_SHARD_UNASSIGNED:
    allowed = to == FLOWIE_CLUSTER_SHARD_CLAIMING;
    break;
  case FLOWIE_CLUSTER_SHARD_CLAIMING:
    allowed = to == FLOWIE_CLUSTER_SHARD_UNASSIGNED || to == FLOWIE_CLUSTER_SHARD_RECOVERING ||
              to == FLOWIE_CLUSTER_SHARD_FENCED;
    break;
  case FLOWIE_CLUSTER_SHARD_RECOVERING:
    allowed = to == FLOWIE_CLUSTER_SHARD_ACTIVE || to == FLOWIE_CLUSTER_SHARD_DRAINING ||
              to == FLOWIE_CLUSTER_SHARD_FENCED;
    break;
  case FLOWIE_CLUSTER_SHARD_ACTIVE:
    allowed = to == FLOWIE_CLUSTER_SHARD_DRAINING || to == FLOWIE_CLUSTER_SHARD_FENCED;
    break;
  case FLOWIE_CLUSTER_SHARD_DRAINING:
    allowed = to == FLOWIE_CLUSTER_SHARD_RELEASED || to == FLOWIE_CLUSTER_SHARD_FENCED;
    break;
  case FLOWIE_CLUSTER_SHARD_RELEASED:
  case FLOWIE_CLUSTER_SHARD_FENCED:
    allowed = to == FLOWIE_CLUSTER_SHARD_UNASSIGNED;
    break;
  }
  return allowed ? TURBO_OK : TURBO_EBUSY;
}

int flowie_cluster_connection_transition_validate(flowie_cluster_connection_state_t from,
                                                  flowie_cluster_connection_state_t to) {
  int allowed = 0;
  if (!flowie_cluster_connection_state_valid(from) || !flowie_cluster_connection_state_valid(to)) {
    return TURBO_EINVAL;
  }
  if (from == to) return TURBO_EALREADY;
  switch (from) {
  case FLOWIE_CLUSTER_CONNECTION_ACCEPTED:
    allowed = to == FLOWIE_CLUSTER_CONNECTION_AUTHENTICATING ||
              to == FLOWIE_CLUSTER_CONNECTION_CLOSING || to == FLOWIE_CLUSTER_CONNECTION_FAILED;
    break;
  case FLOWIE_CLUSTER_CONNECTION_AUTHENTICATING:
    allowed = to == FLOWIE_CLUSTER_CONNECTION_BINDING || to == FLOWIE_CLUSTER_CONNECTION_CLOSING ||
              to == FLOWIE_CLUSTER_CONNECTION_FAILED;
    break;
  case FLOWIE_CLUSTER_CONNECTION_BINDING:
    allowed = to == FLOWIE_CLUSTER_CONNECTION_ACTIVE || to == FLOWIE_CLUSTER_CONNECTION_CLOSING ||
              to == FLOWIE_CLUSTER_CONNECTION_FAILED;
    break;
  case FLOWIE_CLUSTER_CONNECTION_ACTIVE:
    allowed = to == FLOWIE_CLUSTER_CONNECTION_CLOSING || to == FLOWIE_CLUSTER_CONNECTION_FAILED;
    break;
  case FLOWIE_CLUSTER_CONNECTION_CLOSING:
    allowed = to == FLOWIE_CLUSTER_CONNECTION_CLOSED;
    break;
  case FLOWIE_CLUSTER_CONNECTION_FAILED:
    allowed = to == FLOWIE_CLUSTER_CONNECTION_CLOSING || to == FLOWIE_CLUSTER_CONNECTION_CLOSED;
    break;
  case FLOWIE_CLUSTER_CONNECTION_CLOSED:
    break;
  }
  return allowed ? TURBO_OK : TURBO_EBUSY;
}

int flowie_cluster_lease_deadline_ns(uint64_t request_start_ns, uint64_t returned_validity_ms,
                                     uint64_t safety_margin_ms, uint64_t *out_deadline_ns) {
  uint64_t safe_validity_ms;
  uint64_t safe_validity_ns;
  int rc;
  if (out_deadline_ns) *out_deadline_ns = 0u;
  if (!out_deadline_ns || returned_validity_ms == 0u || returned_validity_ms <= safety_margin_ms) {
    return TURBO_EINVAL;
  }
  safe_validity_ms = returned_validity_ms - safety_margin_ms;
  rc = flowie_cluster_u64_mul(safe_validity_ms, FLOWIE_CLUSTER_NS_PER_MS, &safe_validity_ns);
  if (rc != TURBO_OK) return rc;
  return flowie_cluster_u64_add(request_start_ns, safe_validity_ns, out_deadline_ns);
}

int flowie_cluster_required_queue_entries(uint64_t peak_commands_per_second,
                                          uint64_t worst_peer_stall_ms, uint64_t max_inflight_batch,
                                          uint64_t *out_entries) {
  uint64_t product;
  uint64_t stalled_entries;
  int rc;
  if (out_entries) *out_entries = 0u;
  if (!out_entries || peak_commands_per_second == 0u || worst_peer_stall_ms == 0u)
    return TURBO_EINVAL;
  rc = flowie_cluster_u64_mul(peak_commands_per_second, worst_peer_stall_ms, &product);
  if (rc != TURBO_OK) return rc;
  stalled_entries = product / FLOWIE_CLUSTER_MS_PER_SECOND;
  if (product % FLOWIE_CLUSTER_MS_PER_SECOND != 0u) {
    rc = flowie_cluster_u64_add(stalled_entries, 1u, &stalled_entries);
    if (rc != TURBO_OK) return rc;
  }
  return flowie_cluster_u64_add(stalled_entries, max_inflight_batch, out_entries);
}

int flowie_cluster_owner_token_init(flowie_cluster_owner_token_t *out, uint32_t shard_id,
                                    uint64_t owner_epoch, const char *node_id, size_t node_id_size,
                                    const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  if (!out || !node_id || node_id_size == 0u || node_id_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      owner_epoch == 0u || !flowie_cluster_boot_id_valid(boot_id)) {
    return TURBO_EINVAL;
  }
  memset(out, 0, sizeof(*out));
  out->size = sizeof(*out);
  out->abi_version = FLOWIE_CLUSTER_INTERNAL_ABI_V1;
  out->shard_id = shard_id;
  out->owner_epoch = owner_epoch;
  out->node_id_size = node_id_size;
  memcpy(out->node_id, node_id, node_id_size);
  memcpy(out->boot_id, boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE);
  return TURBO_OK;
}

int flowie_cluster_owner_token_require(const flowie_cluster_owner_token_t *expected,
                                       const flowie_cluster_owner_token_t *presented) {
  if (!flowie_cluster_owner_token_valid(expected) || !flowie_cluster_owner_token_valid(presented))
    return TURBO_EINVAL;
  if (expected->shard_id != presented->shard_id ||
      expected->owner_epoch != presented->owner_epoch ||
      expected->node_id_size != presented->node_id_size ||
      memcmp(expected->node_id, presented->node_id, expected->node_id_size) != 0 ||
      memcmp(expected->boot_id, presented->boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0) {
    return TURBO_EBUSY;
  }
  return TURBO_OK;
}

int flowie_cluster_shard_lease_claim(flowie_cluster_shard_lease_t *lease, uint64_t database_now_ms,
                                     uint64_t lease_ttl_ms, uint32_t shard_id, const char *node_id,
                                     size_t node_id_size,
                                     const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                     flowie_cluster_owner_token_t *out) {
  flowie_cluster_owner_token_t claimed = FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
  uint64_t next_epoch;
  uint64_t lease_until;
  int rc;
  if (out) memset(out, 0, sizeof(*out));
  if (!flowie_cluster_shard_lease_valid(lease) || !out || lease_ttl_ms == 0u) return TURBO_EINVAL;
  if (lease->owned && database_now_ms < lease->lease_until_db_ms) return TURBO_EBUSY;
  if (lease->owned && lease->owner.shard_id != shard_id) return TURBO_EPROTO;
  rc = flowie_cluster_u64_add(lease->owner.owner_epoch, 1u, &next_epoch);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_u64_add(database_now_ms, lease_ttl_ms, &lease_until);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_owner_token_init(&claimed, shard_id, next_epoch, node_id, node_id_size,
                                       boot_id);
  if (rc != TURBO_OK) return rc;
  lease->owned = 1u;
  lease->lease_until_db_ms = lease_until;
  lease->owner = claimed;
  *out = claimed;
  return TURBO_OK;
}

int flowie_cluster_shard_lease_require(const flowie_cluster_shard_lease_t *lease,
                                       uint64_t database_now_ms,
                                       const flowie_cluster_owner_token_t *presented) {
  if (!flowie_cluster_shard_lease_valid(lease) || !presented) return TURBO_EINVAL;
  if (!lease->owned || database_now_ms >= lease->lease_until_db_ms) return TURBO_EBUSY;
  return flowie_cluster_owner_token_require(&lease->owner, presented);
}

int flowie_cluster_shard_lease_renew(flowie_cluster_shard_lease_t *lease, uint64_t database_now_ms,
                                     uint64_t lease_ttl_ms,
                                     const flowie_cluster_owner_token_t *presented,
                                     uint64_t *out_lease_until_db_ms) {
  uint64_t lease_until;
  int rc;
  if (out_lease_until_db_ms) *out_lease_until_db_ms = 0u;
  if (!out_lease_until_db_ms || lease_ttl_ms == 0u) return TURBO_EINVAL;
  rc = flowie_cluster_shard_lease_require(lease, database_now_ms, presented);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_u64_add(database_now_ms, lease_ttl_ms, &lease_until);
  if (rc != TURBO_OK) return rc;
  lease->lease_until_db_ms = lease_until;
  *out_lease_until_db_ms = lease_until;
  return TURBO_OK;
}

int flowie_cluster_shard_lease_release(flowie_cluster_shard_lease_t *lease,
                                       uint64_t database_now_ms,
                                       const flowie_cluster_owner_token_t *presented) {
  int rc = flowie_cluster_shard_lease_require(lease, database_now_ms, presented);
  if (rc != TURBO_OK) return rc;
  lease->owned = 0u;
  lease->lease_until_db_ms = 0u;
  return TURBO_OK;
}

int flowie_cluster_runtime_create_local(uint64_t endpoint_instance_id,
                                        flowie_cluster_runtime_t **out) {
  static const uint8_t cluster_id[] = "local-cluster";
  static const uint8_t listener_id[] = "local-listener";
  flowie_cluster_runtime_t *runtime;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0u};
  char node_id[FLOWIE_CLUSTER_NODE_ID_MAX + 1u];
  int node_id_size;
  int rc;
  if (out) *out = NULL;
  if (!out || endpoint_instance_id == 0u) return TURBO_EINVAL;
  node_id_size =
      snprintf(node_id, sizeof(node_id), "local-%llu", (unsigned long long)endpoint_instance_id);
  if (node_id_size <= 0 || (size_t)node_id_size >= sizeof(node_id)) return TURBO_ERANGE;
  for (size_t i = 0u; i < sizeof(endpoint_instance_id); ++i)
    boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE - 1u - i] = (uint8_t)(endpoint_instance_id >> (i * 8u));
  boot_id[0] = 1u;
  runtime = (flowie_cluster_runtime_t *)calloc(1u, sizeof(*runtime));
  if (!runtime) return TURBO_ENOMEM;
  memcpy(runtime->cluster_id, cluster_id, sizeof(cluster_id) - 1u);
  runtime->cluster_id_size = sizeof(cluster_id) - 1u;
  memcpy(runtime->listener_id, listener_id, sizeof(listener_id) - 1u);
  runtime->listener_id_size = sizeof(listener_id) - 1u;
  rc = flowie_cluster_owner_token_init(&runtime->local_owner, 0u, 1u, node_id, (size_t)node_id_size,
                                       boot_id);
  if (rc != TURBO_OK) {
    free(runtime);
    return rc;
  }
  *out = runtime;
  return TURBO_OK;
}

void flowie_cluster_runtime_destroy(flowie_cluster_runtime_t *runtime) { free(runtime); }

int flowie_cluster_runtime_owner_for_key(const flowie_cluster_runtime_t *runtime,
                                         flowie_cluster_key_kind_t kind, const uint8_t *key,
                                         size_t key_size, flowie_cluster_owner_token_t *out) {
  uint32_t shard_id;
  int rc;
  if (out) memset(out, 0, sizeof(*out));
  if (!runtime || !out) return TURBO_EINVAL;
  rc = flowie_cluster_shard_for_key(FLOWIE_CLUSTER_HASH_VERSION_1, kind, runtime->cluster_id,
                                    runtime->cluster_id_size, runtime->listener_id,
                                    runtime->listener_id_size, key, key_size, 1u, &shard_id);
  if (rc != TURBO_OK) return rc;
  if (shard_id != runtime->local_owner.shard_id) return TURBO_EPROTO;
  *out = runtime->local_owner;
  return TURBO_OK;
}
