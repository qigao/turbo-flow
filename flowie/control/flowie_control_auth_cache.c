#include "flowie_control_auth_cache_internal.h"

#include "flowie_control_credential_internal.h"

#include "platform.h"
#include "monocypher.h"
#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_thread.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CONTROL_AUTH_CACHE_DIGEST_SIZE 32u
#define FLOWIE_CONTROL_AUTH_CACHE_KEY_SIZE 32u
#define FLOWIE_CONTROL_AUTH_CACHE_DOMAIN "flowie-control-auth-cache-v1"

typedef struct flowie_control_auth_cache_entry_s {
  uint64_t user_revision;
  uint64_t credential_revision;
  uint64_t expires_at_ms;
  uint64_t last_used;
} flowie_control_auth_cache_entry_t;

struct flowie_control_auth_cache_s {
  turbo_hash_map_t entries;
  turbo_mutex_t lock;
  uint8_t digest_key[FLOWIE_CONTROL_AUTH_CACHE_KEY_SIZE];
  size_t capacity;
  uint64_t ttl_ms;
  uint64_t access_sequence;
  flowie_control_auth_cache_clock_fn clock_ms;
  void *clock_ctx;
};

static uint64_t flowie_control_auth_cache_default_clock(void *ctx) {
  (void)ctx;
  return turbo_monotonic_ms();
}

static int flowie_control_auth_cache_text_valid(const char *value) {
  size_t size;
  if (!value) return 0;
  size = strnlen(value, TURBO_FLOW_SECURITY_ID_MAX + 1u);
  return size > 0u && size <= TURBO_FLOW_SECURITY_ID_MAX;
}

static void flowie_control_auth_cache_hash_u64(crypto_blake2b_ctx *ctx, uint64_t value) {
  uint8_t encoded[8];
  for (size_t index = 0u; index < sizeof(encoded); ++index)
    encoded[index] = (uint8_t)(value >> (index * 8u));
  crypto_blake2b_update(ctx, encoded, sizeof(encoded));
  flowie_control_credential_wipe(encoded, sizeof(encoded));
}

static void flowie_control_auth_cache_digest(
    const flowie_control_auth_cache_t *cache, const char *root_group_id, const char *principal_id,
    const void *secret, size_t secret_size, uint8_t digest[FLOWIE_CONTROL_AUTH_CACHE_DIGEST_SIZE]) {
  crypto_blake2b_ctx hash;
  size_t root_size = strlen(root_group_id);
  size_t principal_size = strlen(principal_id);
  crypto_blake2b_keyed_init(&hash, FLOWIE_CONTROL_AUTH_CACHE_DIGEST_SIZE, cache->digest_key,
                            sizeof(cache->digest_key));
  crypto_blake2b_update(&hash, (const uint8_t *)FLOWIE_CONTROL_AUTH_CACHE_DOMAIN,
                        sizeof(FLOWIE_CONTROL_AUTH_CACHE_DOMAIN) - 1u);
  flowie_control_auth_cache_hash_u64(&hash, root_size);
  crypto_blake2b_update(&hash, (const uint8_t *)root_group_id, root_size);
  flowie_control_auth_cache_hash_u64(&hash, principal_size);
  crypto_blake2b_update(&hash, (const uint8_t *)principal_id, principal_size);
  flowie_control_auth_cache_hash_u64(&hash, secret_size);
  crypto_blake2b_update(&hash, (const uint8_t *)secret, secret_size);
  crypto_blake2b_final(&hash, digest);
}

static uint64_t flowie_control_auth_cache_expiry(uint64_t now_ms, uint64_t ttl_ms) {
  return now_ms > UINT64_MAX - ttl_ms ? UINT64_MAX : now_ms + ttl_ms;
}

static uint64_t flowie_control_auth_cache_next_sequence(flowie_control_auth_cache_t *cache) {
  if (cache->access_sequence == UINT64_MAX) {
    uint64_t minimum = UINT64_MAX;
    size_t map_capacity = turbo_hash_map_capacity(&cache->entries);
    for (size_t slot = 0u; slot < map_capacity; ++slot) {
      flowie_control_auth_cache_entry_t *entry =
          (flowie_control_auth_cache_entry_t *)turbo_hash_map_value_at(&cache->entries, slot);
      if (entry && entry->last_used < minimum) minimum = entry->last_used;
    }
    if (minimum == 0u || minimum == UINT64_MAX) minimum = 1u;
    for (size_t slot = 0u; slot < map_capacity; ++slot) {
      flowie_control_auth_cache_entry_t *entry =
          (flowie_control_auth_cache_entry_t *)turbo_hash_map_value_at(&cache->entries, slot);
      if (entry) entry->last_used -= minimum - 1u;
    }
    cache->access_sequence -= minimum - 1u;
  }
  return ++cache->access_sequence;
}

static void flowie_control_auth_cache_remove_locked(
    flowie_control_auth_cache_t *cache,
    const uint8_t digest[FLOWIE_CONTROL_AUTH_CACHE_DIGEST_SIZE]) {
  flowie_control_auth_cache_entry_t removed = {0};
  if (turbo_hash_map_remove(&cache->entries, digest, &removed) == TURBO_OK)
    flowie_control_credential_wipe(&removed, sizeof(removed));
}

static void flowie_control_auth_cache_evict_locked(flowie_control_auth_cache_t *cache) {
  uint8_t oldest_digest[FLOWIE_CONTROL_AUTH_CACHE_DIGEST_SIZE] = {0};
  uint64_t oldest_sequence = UINT64_MAX;
  int found = 0;
  size_t map_capacity = turbo_hash_map_capacity(&cache->entries);
  for (size_t slot = 0u; slot < map_capacity; ++slot) {
    const uint8_t *digest = (const uint8_t *)turbo_hash_map_key_at(&cache->entries, slot);
    const flowie_control_auth_cache_entry_t *entry =
        (const flowie_control_auth_cache_entry_t *)turbo_hash_map_value_at_const(&cache->entries,
                                                                                 slot);
    if (digest && entry && (!found || entry->last_used < oldest_sequence)) {
      memcpy(oldest_digest, digest, sizeof(oldest_digest));
      oldest_sequence = entry->last_used;
      found = 1;
    }
  }
  if (found) flowie_control_auth_cache_remove_locked(cache, oldest_digest);
  flowie_control_credential_wipe(oldest_digest, sizeof(oldest_digest));
}

static void flowie_control_auth_cache_store(
    flowie_control_auth_cache_t *cache, const uint8_t digest[FLOWIE_CONTROL_AUTH_CACHE_DIGEST_SIZE],
    const flowie_control_credential_verify_result_t *verified, uint64_t now_ms) {
  flowie_control_auth_cache_entry_t entry;
  flowie_control_auth_cache_entry_t *existing;
  turbo_mutex_lock(&cache->lock);
  existing = (flowie_control_auth_cache_entry_t *)turbo_hash_map_get(&cache->entries, digest);
  if (existing) {
    existing->user_revision = verified->user_revision;
    existing->credential_revision = verified->credential_revision;
    existing->expires_at_ms = flowie_control_auth_cache_expiry(now_ms, cache->ttl_ms);
    existing->last_used = flowie_control_auth_cache_next_sequence(cache);
    turbo_mutex_unlock(&cache->lock);
    return;
  }
  if (turbo_hash_map_size(&cache->entries) >= cache->capacity)
    flowie_control_auth_cache_evict_locked(cache);
  entry.user_revision = verified->user_revision;
  entry.credential_revision = verified->credential_revision;
  entry.expires_at_ms = flowie_control_auth_cache_expiry(now_ms, cache->ttl_ms);
  entry.last_used = flowie_control_auth_cache_next_sequence(cache);
  (void)turbo_hash_map_put(&cache->entries, digest, &entry);
  turbo_mutex_unlock(&cache->lock);
  flowie_control_credential_wipe(&entry, sizeof(entry));
}

int flowie_control_auth_cache_create(const flowie_control_auth_cache_config_t *config,
                                     flowie_control_auth_cache_t **out) {
  flowie_control_auth_cache_t *cache = NULL;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out || config->capacity == 0u ||
      config->capacity > FLOWIE_CONTROL_AUTH_CACHE_MAX_CAPACITY || config->ttl_ms == 0u ||
      config->ttl_ms > FLOWIE_CONTROL_AUTH_CACHE_MAX_TTL_MS)
    return TURBO_EINVAL;
  cache = (flowie_control_auth_cache_t *)calloc(1u, sizeof(*cache));
  if (!cache) return TURBO_ENOMEM;
  cache->capacity = config->capacity;
  cache->ttl_ms = config->ttl_ms;
  cache->clock_ms = config->clock_ms ? config->clock_ms : flowie_control_auth_cache_default_clock;
  cache->clock_ctx = config->clock_ctx;
  rc = turbo_secure_random(cache->digest_key, sizeof(cache->digest_key));
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_init(&cache->entries, FLOWIE_CONTROL_AUTH_CACHE_DIGEST_SIZE,
                           sizeof(flowie_control_auth_cache_entry_t), NULL, NULL, NULL);
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

void flowie_control_auth_cache_destroy(flowie_control_auth_cache_t *cache) {
  size_t map_capacity;
  if (!cache) return;
  turbo_mutex_lock(&cache->lock);
  map_capacity = turbo_hash_map_capacity(&cache->entries);
  for (size_t slot = 0u; slot < map_capacity; ++slot) {
    flowie_control_auth_cache_entry_t *entry =
        (flowie_control_auth_cache_entry_t *)turbo_hash_map_value_at(&cache->entries, slot);
    if (entry) flowie_control_credential_wipe(entry, sizeof(*entry));
  }
  turbo_hash_map_clear(&cache->entries);
  turbo_mutex_unlock(&cache->lock);
  turbo_mutex_destroy(&cache->lock);
  turbo_hash_map_destroy(&cache->entries);
  flowie_control_credential_wipe(cache, sizeof(*cache));
  free(cache);
}

int flowie_control_auth_cache_verify(flowie_control_auth_cache_t *cache,
                                     flowie_control_store_t *store, const char *root_group_id,
                                     const char *principal_id, const void *secret,
                                     size_t secret_size,
                                     flowie_control_credential_verify_result_t *result,
                                     int *cache_hit_out) {
  flowie_control_credential_verify_result_t cached = FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  flowie_control_credential_verify_result_t current = FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  uint8_t digest[FLOWIE_CONTROL_AUTH_CACHE_DIGEST_SIZE] = {0};
  uint64_t now_ms;
  int candidate = 0;
  int rc;
  if (result && result->size >= sizeof(*result))
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  if (cache_hit_out) *cache_hit_out = 0;
  if (!cache || !store || !flowie_control_auth_cache_text_valid(root_group_id) ||
      !flowie_control_auth_cache_text_valid(principal_id) || !secret || secret_size == 0u ||
      secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX || !result ||
      result->size < sizeof(*result) || !cache_hit_out)
    return TURBO_EINVAL;
  flowie_control_auth_cache_digest(cache, root_group_id, principal_id, secret, secret_size, digest);
  now_ms = cache->clock_ms(cache->clock_ctx);
  turbo_mutex_lock(&cache->lock);
  {
    flowie_control_auth_cache_entry_t *entry =
        (flowie_control_auth_cache_entry_t *)turbo_hash_map_get(&cache->entries, digest);
    if (entry && now_ms < entry->expires_at_ms) {
      cached.user_revision = entry->user_revision;
      cached.credential_revision = entry->credential_revision;
      candidate = 1;
    } else if (entry) {
      flowie_control_auth_cache_remove_locked(cache, digest);
    }
  }
  turbo_mutex_unlock(&cache->lock);
  if (candidate) {
    rc = flowie_control_store_credential_state(store, root_group_id, principal_id, &current);
    if (rc != TURBO_OK) {
      turbo_mutex_lock(&cache->lock);
      flowie_control_auth_cache_remove_locked(cache, digest);
      turbo_mutex_unlock(&cache->lock);
      goto done;
    }
    if (current.user_revision == cached.user_revision &&
        current.credential_revision == cached.credential_revision) {
      turbo_mutex_lock(&cache->lock);
      {
        flowie_control_auth_cache_entry_t *entry =
            (flowie_control_auth_cache_entry_t *)turbo_hash_map_get(&cache->entries, digest);
        if (entry && entry->user_revision == cached.user_revision &&
            entry->credential_revision == cached.credential_revision &&
            now_ms < entry->expires_at_ms)
          entry->last_used = flowie_control_auth_cache_next_sequence(cache);
      }
      turbo_mutex_unlock(&cache->lock);
      *result = current;
      *cache_hit_out = 1;
      rc = TURBO_OK;
      goto done;
    }
    turbo_mutex_lock(&cache->lock);
    flowie_control_auth_cache_remove_locked(cache, digest);
    turbo_mutex_unlock(&cache->lock);
  }
  rc = flowie_control_store_credential_verify(store, root_group_id, principal_id, secret,
                                              secret_size, &current);
  if (rc == TURBO_OK) {
    flowie_control_auth_cache_store(cache, digest, &current, now_ms);
    *result = current;
  }

done:
  flowie_control_credential_wipe(digest, sizeof(digest));
  if (rc != TURBO_OK)
    *result =
        (flowie_control_credential_verify_result_t)FLOWIE_CONTROL_CREDENTIAL_VERIFY_RESULT_INIT;
  return rc;
}

size_t flowie_control_auth_cache_size(flowie_control_auth_cache_t *cache) {
  size_t size;
  if (!cache) return 0u;
  turbo_mutex_lock(&cache->lock);
  size = turbo_hash_map_size(&cache->entries);
  turbo_mutex_unlock(&cache->lock);
  return size;
}
