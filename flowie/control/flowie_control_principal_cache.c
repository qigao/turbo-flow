#include "flowie_control_principal_cache_internal.h"

#include "flowie_control_credential_internal.h"

#include "monocypher.h"
#include "platform.h"
#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_thread.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CONTROL_PRINCIPAL_CACHE_DIGEST_SIZE 32u
#define FLOWIE_CONTROL_PRINCIPAL_CACHE_KEY_SIZE 32u
#define FLOWIE_CONTROL_PRINCIPAL_CACHE_DOMAIN "flowie-control-principal-cache-v1"

typedef struct flowie_control_principal_cache_entry_s {
  flowie_control_principal_snapshot_t snapshot;
  uint64_t store_revision;
  uint64_t policy_version;
  uint64_t expires_at_ms;
  uint64_t last_used;
} flowie_control_principal_cache_entry_t;

struct flowie_control_principal_cache_s {
  turbo_hash_map_t entries;
  turbo_mutex_t lock;
  uint8_t digest_key[FLOWIE_CONTROL_PRINCIPAL_CACHE_KEY_SIZE];
  size_t capacity;
  uint64_t ttl_ms;
  uint64_t access_sequence;
  flowie_control_auth_cache_clock_fn clock_ms;
  void *clock_ctx;
};

static uint64_t flowie_control_principal_cache_default_clock(void *ctx) {
  (void)ctx;
  return turbo_monotonic_ms();
}

static int flowie_control_principal_cache_text_valid(const char *value) {
  size_t size;
  if (!value) return 0;
  size = strnlen(value, TURBO_FLOW_SECURITY_ID_MAX + 1u);
  return size > 0u && size <= TURBO_FLOW_SECURITY_ID_MAX;
}

static void flowie_control_principal_cache_hash_u64(crypto_blake2b_ctx *ctx, uint64_t value) {
  uint8_t encoded[8];
  for (size_t index = 0u; index < sizeof(encoded); ++index)
    encoded[index] = (uint8_t)(value >> (index * 8u));
  crypto_blake2b_update(ctx, encoded, sizeof(encoded));
  flowie_control_credential_wipe(encoded, sizeof(encoded));
}

static void flowie_control_principal_cache_digest(
    const flowie_control_principal_cache_t *cache, const char *domain_id,
    const char *principal_id, uint8_t digest[FLOWIE_CONTROL_PRINCIPAL_CACHE_DIGEST_SIZE]) {
  crypto_blake2b_ctx hash;
  size_t root_size = strlen(domain_id);
  size_t principal_size = strlen(principal_id);
  crypto_blake2b_keyed_init(&hash, FLOWIE_CONTROL_PRINCIPAL_CACHE_DIGEST_SIZE,
                            cache->digest_key, sizeof(cache->digest_key));
  crypto_blake2b_update(&hash, (const uint8_t *)FLOWIE_CONTROL_PRINCIPAL_CACHE_DOMAIN,
                        sizeof(FLOWIE_CONTROL_PRINCIPAL_CACHE_DOMAIN) - 1u);
  flowie_control_principal_cache_hash_u64(&hash, root_size);
  crypto_blake2b_update(&hash, (const uint8_t *)domain_id, root_size);
  flowie_control_principal_cache_hash_u64(&hash, principal_size);
  crypto_blake2b_update(&hash, (const uint8_t *)principal_id, principal_size);
  crypto_blake2b_final(&hash, digest);
}

static uint64_t flowie_control_principal_cache_expiry(uint64_t now_ms, uint64_t ttl_ms) {
  return now_ms > UINT64_MAX - ttl_ms ? UINT64_MAX : now_ms + ttl_ms;
}

static uint64_t flowie_control_principal_cache_next_sequence(
    flowie_control_principal_cache_t *cache) {
  if (cache->access_sequence == UINT64_MAX) {
    uint64_t minimum = UINT64_MAX;
    size_t map_capacity = turbo_hash_map_capacity(&cache->entries);
    for (size_t slot = 0u; slot < map_capacity; ++slot) {
      flowie_control_principal_cache_entry_t *entry =
          (flowie_control_principal_cache_entry_t *)turbo_hash_map_value_at(&cache->entries, slot);
      if (entry && entry->last_used < minimum) minimum = entry->last_used;
    }
    if (minimum == 0u || minimum == UINT64_MAX) minimum = 1u;
    for (size_t slot = 0u; slot < map_capacity; ++slot) {
      flowie_control_principal_cache_entry_t *entry =
          (flowie_control_principal_cache_entry_t *)turbo_hash_map_value_at(&cache->entries, slot);
      if (entry) entry->last_used -= minimum - 1u;
    }
    cache->access_sequence -= minimum - 1u;
  }
  return ++cache->access_sequence;
}

static void flowie_control_principal_cache_remove_locked(
    flowie_control_principal_cache_t *cache,
    const uint8_t digest[FLOWIE_CONTROL_PRINCIPAL_CACHE_DIGEST_SIZE]) {
  flowie_control_principal_cache_entry_t removed;
  memset(&removed, 0, sizeof(removed));
  if (turbo_hash_map_remove(&cache->entries, digest, &removed) == TURBO_OK)
    flowie_control_credential_wipe(&removed, sizeof(removed));
}

static void flowie_control_principal_cache_evict_locked(flowie_control_principal_cache_t *cache) {
  uint8_t oldest_digest[FLOWIE_CONTROL_PRINCIPAL_CACHE_DIGEST_SIZE] = {0};
  uint64_t oldest_sequence = UINT64_MAX;
  int found = 0;
  size_t map_capacity = turbo_hash_map_capacity(&cache->entries);
  for (size_t slot = 0u; slot < map_capacity; ++slot) {
    const uint8_t *digest = (const uint8_t *)turbo_hash_map_key_at(&cache->entries, slot);
    const flowie_control_principal_cache_entry_t *entry =
        (const flowie_control_principal_cache_entry_t *)turbo_hash_map_value_at_const(
            &cache->entries, slot);
    if (digest && entry && (!found || entry->last_used < oldest_sequence)) {
      memcpy(oldest_digest, digest, sizeof(oldest_digest));
      oldest_sequence = entry->last_used;
      found = 1;
    }
  }
  if (found) flowie_control_principal_cache_remove_locked(cache, oldest_digest);
  flowie_control_credential_wipe(oldest_digest, sizeof(oldest_digest));
}

int flowie_control_principal_cache_create(const flowie_control_auth_cache_config_t *config,
                                          flowie_control_principal_cache_t **out) {
  flowie_control_principal_cache_t *cache = NULL;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out || config->capacity == 0u ||
      config->capacity > FLOWIE_CONTROL_AUTH_CACHE_MAX_CAPACITY || config->ttl_ms == 0u ||
      config->ttl_ms > FLOWIE_CONTROL_AUTH_CACHE_MAX_TTL_MS)
    return TURBO_EINVAL;
  cache = (flowie_control_principal_cache_t *)calloc(1u, sizeof(*cache));
  if (!cache) return TURBO_ENOMEM;
  cache->capacity = config->capacity;
  cache->ttl_ms = config->ttl_ms;
  cache->clock_ms = config->clock_ms ? config->clock_ms : flowie_control_principal_cache_default_clock;
  cache->clock_ctx = config->clock_ctx;
  rc = turbo_secure_random(cache->digest_key, sizeof(cache->digest_key));
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_init(&cache->entries, FLOWIE_CONTROL_PRINCIPAL_CACHE_DIGEST_SIZE,
                           sizeof(flowie_control_principal_cache_entry_t), NULL, NULL, NULL);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_reserve(&cache->entries, cache->capacity);
  if (rc != TURBO_OK) goto fail;
  turbo_mutex_init(&cache->lock);
  *out = cache;
  return TURBO_OK;

fail:
  turbo_hash_map_destroy(&cache->entries);
  flowie_control_credential_wipe(cache, sizeof(*cache));
  free(cache);
  return rc;
}

void flowie_control_principal_cache_destroy(flowie_control_principal_cache_t *cache) {
  size_t map_capacity;
  if (!cache) return;
  turbo_mutex_lock(&cache->lock);
  map_capacity = turbo_hash_map_capacity(&cache->entries);
  for (size_t slot = 0u; slot < map_capacity; ++slot) {
    flowie_control_principal_cache_entry_t *entry =
        (flowie_control_principal_cache_entry_t *)turbo_hash_map_value_at(&cache->entries, slot);
    if (entry) flowie_control_credential_wipe(entry, sizeof(*entry));
  }
  turbo_hash_map_clear(&cache->entries);
  turbo_mutex_unlock(&cache->lock);
  turbo_mutex_destroy(&cache->lock);
  turbo_hash_map_destroy(&cache->entries);
  flowie_control_credential_wipe(cache, sizeof(*cache));
  free(cache);
}

int flowie_control_principal_cache_get(
    flowie_control_principal_cache_t *cache, const char *domain_id,
    const char *principal_id, uint64_t user_revision, uint64_t credential_revision,
    uint64_t store_revision, uint64_t policy_version, flowie_control_principal_snapshot_t *out,
    int *cache_hit_out) {
  uint8_t digest[FLOWIE_CONTROL_PRINCIPAL_CACHE_DIGEST_SIZE] = {0};
  uint64_t now_ms;
  int rc = TURBO_ENOENT;
  if (out && out->size >= sizeof(*out))
    *out = (flowie_control_principal_snapshot_t)FLOWIE_CONTROL_PRINCIPAL_SNAPSHOT_INIT;
  if (cache_hit_out) *cache_hit_out = 0;
  if (!cache || !flowie_control_principal_cache_text_valid(domain_id) ||
      !flowie_control_principal_cache_text_valid(principal_id) || user_revision == 0u ||
      credential_revision == 0u || store_revision == 0u || policy_version == 0u || !out ||
      out->size < sizeof(*out) || !cache_hit_out)
    return TURBO_EINVAL;
  flowie_control_principal_cache_digest(cache, domain_id, principal_id, digest);
  now_ms = cache->clock_ms(cache->clock_ctx);
  turbo_mutex_lock(&cache->lock);
  {
    flowie_control_principal_cache_entry_t *entry =
        (flowie_control_principal_cache_entry_t *)turbo_hash_map_get(&cache->entries, digest);
    if (!entry || now_ms >= entry->expires_at_ms || entry->snapshot.user_revision != user_revision ||
        entry->snapshot.credential_revision != credential_revision ||
        entry->store_revision != store_revision || entry->policy_version != policy_version) {
      if (entry) flowie_control_principal_cache_remove_locked(cache, digest);
    } else {
      *out = entry->snapshot;
      entry->last_used = flowie_control_principal_cache_next_sequence(cache);
      *cache_hit_out = 1;
      rc = TURBO_OK;
    }
  }
  turbo_mutex_unlock(&cache->lock);
  flowie_control_credential_wipe(digest, sizeof(digest));
  return rc;
}

int flowie_control_principal_cache_put(flowie_control_principal_cache_t *cache,
                                       const flowie_control_principal_snapshot_t *snapshot,
                                       uint64_t store_revision, uint64_t policy_version) {
  uint8_t digest[FLOWIE_CONTROL_PRINCIPAL_CACHE_DIGEST_SIZE] = {0};
  flowie_control_principal_cache_entry_t entry;
  flowie_control_principal_cache_entry_t *existing;
  uint64_t now_ms;
  int rc = TURBO_OK;
  if (!cache || !snapshot || snapshot->size < sizeof(*snapshot) ||
      !flowie_control_principal_cache_text_valid(snapshot->domain_id) ||
      !flowie_control_principal_cache_text_valid(snapshot->principal_id) ||
      snapshot->user_revision == 0u || snapshot->credential_revision == 0u || store_revision == 0u ||
      policy_version == 0u)
    return TURBO_EINVAL;
  flowie_control_principal_cache_digest(cache, snapshot->domain_id, snapshot->principal_id,
                                        digest);
  now_ms = cache->clock_ms(cache->clock_ctx);
  memset(&entry, 0, sizeof(entry));
  entry.snapshot = *snapshot;
  entry.store_revision = store_revision;
  entry.policy_version = policy_version;
  entry.expires_at_ms = flowie_control_principal_cache_expiry(now_ms, cache->ttl_ms);
  turbo_mutex_lock(&cache->lock);
  existing = (flowie_control_principal_cache_entry_t *)turbo_hash_map_get(&cache->entries, digest);
  if (existing) {
    *existing = entry;
    existing->last_used = flowie_control_principal_cache_next_sequence(cache);
  } else {
    if (turbo_hash_map_size(&cache->entries) >= cache->capacity)
      flowie_control_principal_cache_evict_locked(cache);
    entry.last_used = flowie_control_principal_cache_next_sequence(cache);
    rc = turbo_hash_map_put(&cache->entries, digest, &entry);
  }
  turbo_mutex_unlock(&cache->lock);
  flowie_control_credential_wipe(&entry, sizeof(entry));
  flowie_control_credential_wipe(digest, sizeof(digest));
  return rc;
}

size_t flowie_control_principal_cache_size(flowie_control_principal_cache_t *cache) {
  size_t size;
  if (!cache) return 0u;
  turbo_mutex_lock(&cache->lock);
  size = turbo_hash_map_size(&cache->entries);
  turbo_mutex_unlock(&cache->lock);
  return size;
}
