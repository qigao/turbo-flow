#include "flowie_control_auth_rate_limiter_internal.h"

#include "flowie_control_credential_internal.h"

#include "monocypher.h"
#include "platform.h"
#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_thread.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE 32u
#define FLOWIE_CONTROL_AUTH_RATE_KEY_SIZE 32u
#define FLOWIE_CONTROL_AUTH_RATE_TOKEN_UNITS 1000u
#define FLOWIE_CONTROL_AUTH_RATE_CALLER_SCOPE 1u
#define FLOWIE_CONTROL_AUTH_RATE_IDENTITY_SCOPE 2u
#define FLOWIE_CONTROL_AUTH_RATE_DOMAIN "flowie-control-auth-rate-v1"

typedef struct flowie_control_auth_rate_entry_s {
  uint64_t available_units;
  uint64_t last_refill_ms;
  uint64_t last_used;
} flowie_control_auth_rate_entry_t;

struct flowie_control_auth_rate_limiter_s {
  turbo_hash_map_t callers;
  turbo_hash_map_t identities;
  turbo_mutex_t lock;
  uint8_t digest_key[FLOWIE_CONTROL_AUTH_RATE_KEY_SIZE];
  size_t caller_capacity;
  size_t identity_capacity;
  uint32_t caller_per_second;
  uint32_t caller_burst;
  uint32_t identity_per_second;
  uint32_t identity_burst;
  uint64_t access_sequence;
  flowie_control_auth_rate_clock_fn clock_ms;
  void *clock_ctx;
};

static uint64_t flowie_control_auth_rate_default_clock(void *ctx) {
  (void)ctx;
  return turbo_monotonic_ms();
}

static int flowie_control_auth_rate_text_valid(const char *value, size_t maximum) {
  size_t size;
  if (!value) return 0;
  size = strnlen(value, maximum + 1u);
  return size > 0u && size <= maximum;
}

static void flowie_control_auth_rate_hash_text(crypto_blake2b_ctx *hash, const char *value) {
  uint8_t encoded[8];
  size_t size = strlen(value);
  for (size_t index = 0u; index < sizeof(encoded); ++index)
    encoded[index] = (uint8_t)((uint64_t)size >> (index * 8u));
  crypto_blake2b_update(hash, encoded, sizeof(encoded));
  crypto_blake2b_update(hash, (const uint8_t *)value, size);
  flowie_control_credential_wipe(encoded, sizeof(encoded));
}

static void flowie_control_auth_rate_digest(
    const flowie_control_auth_rate_limiter_t *limiter, uint8_t scope,
    const char *peer_certificate_sha256, const char *root_group_id, const char *principal_id,
    uint8_t digest[FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE]) {
  crypto_blake2b_ctx hash;
  crypto_blake2b_keyed_init(&hash, FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE, limiter->digest_key,
                            sizeof(limiter->digest_key));
  crypto_blake2b_update(&hash, (const uint8_t *)FLOWIE_CONTROL_AUTH_RATE_DOMAIN,
                        sizeof(FLOWIE_CONTROL_AUTH_RATE_DOMAIN) - 1u);
  crypto_blake2b_update(&hash, &scope, sizeof(scope));
  flowie_control_auth_rate_hash_text(&hash, peer_certificate_sha256);
  if (scope == FLOWIE_CONTROL_AUTH_RATE_IDENTITY_SCOPE) {
    flowie_control_auth_rate_hash_text(&hash, root_group_id);
    flowie_control_auth_rate_hash_text(&hash, principal_id);
  }
  crypto_blake2b_final(&hash, digest);
}

static uint64_t flowie_control_auth_rate_next_sequence(
    flowie_control_auth_rate_limiter_t *limiter) {
  if (limiter->access_sequence == UINT64_MAX) limiter->access_sequence = 0u;
  return ++limiter->access_sequence;
}

static void flowie_control_auth_rate_remove_locked(
    turbo_hash_map_t *entries, const uint8_t digest[FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE]) {
  flowie_control_auth_rate_entry_t removed = {0};
  if (turbo_hash_map_remove(entries, digest, &removed) == TURBO_OK)
    flowie_control_credential_wipe(&removed, sizeof(removed));
}

static void flowie_control_auth_rate_evict_locked(turbo_hash_map_t *entries) {
  uint8_t oldest_digest[FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE] = {0};
  uint64_t oldest_sequence = UINT64_MAX;
  int found = 0;
  size_t capacity = turbo_hash_map_capacity(entries);
  for (size_t slot = 0u; slot < capacity; ++slot) {
    const uint8_t *digest = (const uint8_t *)turbo_hash_map_key_at(entries, slot);
    const flowie_control_auth_rate_entry_t *entry =
        (const flowie_control_auth_rate_entry_t *)turbo_hash_map_value_at_const(entries, slot);
    if (digest && entry && (!found || entry->last_used < oldest_sequence)) {
      memcpy(oldest_digest, digest, sizeof(oldest_digest));
      oldest_sequence = entry->last_used;
      found = 1;
    }
  }
  if (found) flowie_control_auth_rate_remove_locked(entries, oldest_digest);
  flowie_control_credential_wipe(oldest_digest, sizeof(oldest_digest));
}

static int flowie_control_auth_rate_entry_get_locked(
    flowie_control_auth_rate_limiter_t *limiter, turbo_hash_map_t *entries, size_t capacity,
    const uint8_t digest[FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE], uint32_t burst, uint64_t now_ms,
    flowie_control_auth_rate_entry_t **entry_out) {
  flowie_control_auth_rate_entry_t initial = {0};
  flowie_control_auth_rate_entry_t *entry;
  int rc;
  *entry_out = NULL;
  entry = (flowie_control_auth_rate_entry_t *)turbo_hash_map_get(entries, digest);
  if (entry) {
    *entry_out = entry;
    return TURBO_OK;
  }
  if (turbo_hash_map_size(entries) >= capacity) flowie_control_auth_rate_evict_locked(entries);
  initial.available_units = (uint64_t)burst * FLOWIE_CONTROL_AUTH_RATE_TOKEN_UNITS;
  initial.last_refill_ms = now_ms;
  initial.last_used = flowie_control_auth_rate_next_sequence(limiter);
  rc = turbo_hash_map_put(entries, digest, &initial);
  flowie_control_credential_wipe(&initial, sizeof(initial));
  if (rc != TURBO_OK) return rc;
  *entry_out = (flowie_control_auth_rate_entry_t *)turbo_hash_map_get(entries, digest);
  return *entry_out ? TURBO_OK : TURBO_EIO;
}

static int flowie_control_auth_rate_refill(flowie_control_auth_rate_entry_t *entry,
                                           uint32_t per_second, uint32_t burst,
                                           uint64_t now_ms) {
  uint64_t maximum = (uint64_t)burst * FLOWIE_CONTROL_AUTH_RATE_TOKEN_UNITS;
  uint64_t elapsed;
  uint64_t added;
  if (now_ms < entry->last_refill_ms) return TURBO_EIO;
  elapsed = now_ms - entry->last_refill_ms;
  added = elapsed > UINT64_MAX / per_second ? UINT64_MAX : elapsed * per_second;
  if (added >= maximum - entry->available_units)
    entry->available_units = maximum;
  else
    entry->available_units += added;
  entry->last_refill_ms = now_ms;
  return TURBO_OK;
}

int flowie_control_auth_rate_limiter_create(
    const flowie_control_auth_rate_limiter_config_t *config,
    flowie_control_auth_rate_limiter_t **out) {
  flowie_control_auth_rate_limiter_t *limiter = NULL;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out || config->caller_capacity == 0u ||
      config->caller_capacity > FLOWIE_CONTROL_AUTH_RATE_MAX_CAPACITY ||
      config->identity_capacity == 0u ||
      config->identity_capacity > FLOWIE_CONTROL_AUTH_RATE_MAX_CAPACITY ||
      config->caller_per_second == 0u ||
      config->caller_per_second > FLOWIE_CONTROL_AUTH_RATE_MAX_PER_SECOND ||
      config->identity_per_second == 0u ||
      config->identity_per_second > FLOWIE_CONTROL_AUTH_RATE_MAX_PER_SECOND ||
      config->caller_burst == 0u || config->caller_burst > FLOWIE_CONTROL_AUTH_RATE_MAX_BURST ||
      config->identity_burst == 0u ||
      config->identity_burst > FLOWIE_CONTROL_AUTH_RATE_MAX_BURST)
    return TURBO_EINVAL;
  limiter = (flowie_control_auth_rate_limiter_t *)calloc(1u, sizeof(*limiter));
  if (!limiter) return TURBO_ENOMEM;
  limiter->caller_capacity = config->caller_capacity;
  limiter->identity_capacity = config->identity_capacity;
  limiter->caller_per_second = config->caller_per_second;
  limiter->caller_burst = config->caller_burst;
  limiter->identity_per_second = config->identity_per_second;
  limiter->identity_burst = config->identity_burst;
  limiter->clock_ms = config->clock_ms ? config->clock_ms : flowie_control_auth_rate_default_clock;
  limiter->clock_ctx = config->clock_ctx;
  rc = turbo_secure_random(limiter->digest_key, sizeof(limiter->digest_key));
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_init(&limiter->callers, FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE,
                           sizeof(flowie_control_auth_rate_entry_t), NULL, NULL, NULL);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_reserve(&limiter->callers, limiter->caller_capacity);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_init(&limiter->identities, FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE,
                           sizeof(flowie_control_auth_rate_entry_t), NULL, NULL, NULL);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_reserve(&limiter->identities, limiter->identity_capacity);
  if (rc != TURBO_OK) goto fail;
  turbo_mutex_init(&limiter->lock);
  *out = limiter;
  return TURBO_OK;

fail:
  turbo_hash_map_destroy(&limiter->identities);
  turbo_hash_map_destroy(&limiter->callers);
  flowie_control_credential_wipe(limiter, sizeof(*limiter));
  free(limiter);
  return rc;
}

void flowie_control_auth_rate_limiter_destroy(flowie_control_auth_rate_limiter_t *limiter) {
  if (!limiter) return;
  turbo_mutex_lock(&limiter->lock);
  turbo_hash_map_clear(&limiter->identities);
  turbo_hash_map_clear(&limiter->callers);
  turbo_mutex_unlock(&limiter->lock);
  turbo_mutex_destroy(&limiter->lock);
  turbo_hash_map_destroy(&limiter->identities);
  turbo_hash_map_destroy(&limiter->callers);
  flowie_control_credential_wipe(limiter, sizeof(*limiter));
  free(limiter);
}

int flowie_control_auth_rate_limiter_acquire(flowie_control_auth_rate_limiter_t *limiter,
                                             const char *peer_certificate_sha256,
                                             const char *root_group_id,
                                             const char *principal_id) {
  uint8_t caller_digest[FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE] = {0};
  uint8_t identity_digest[FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE] = {0};
  flowie_control_auth_rate_entry_t *caller = NULL;
  flowie_control_auth_rate_entry_t *identity = NULL;
  uint64_t now_ms;
  int rc;
  if (!limiter ||
      !flowie_control_auth_rate_text_valid(peer_certificate_sha256,
                                           FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE) ||
      !flowie_control_auth_rate_text_valid(root_group_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_auth_rate_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX))
    return TURBO_EINVAL;
  flowie_control_auth_rate_digest(limiter, FLOWIE_CONTROL_AUTH_RATE_CALLER_SCOPE,
                                  peer_certificate_sha256, root_group_id, principal_id,
                                  caller_digest);
  flowie_control_auth_rate_digest(limiter, FLOWIE_CONTROL_AUTH_RATE_IDENTITY_SCOPE,
                                  peer_certificate_sha256, root_group_id, principal_id,
                                  identity_digest);
  now_ms = limiter->clock_ms(limiter->clock_ctx);
  turbo_mutex_lock(&limiter->lock);
  rc = flowie_control_auth_rate_entry_get_locked(
      limiter, &limiter->callers, limiter->caller_capacity, caller_digest,
      limiter->caller_burst, now_ms, &caller);
  if (rc == TURBO_OK)
    rc = flowie_control_auth_rate_entry_get_locked(
        limiter, &limiter->identities, limiter->identity_capacity, identity_digest,
        limiter->identity_burst, now_ms, &identity);
  if (rc == TURBO_OK)
    rc = flowie_control_auth_rate_refill(caller, limiter->caller_per_second,
                                         limiter->caller_burst, now_ms);
  if (rc == TURBO_OK)
    rc = flowie_control_auth_rate_refill(identity, limiter->identity_per_second,
                                         limiter->identity_burst, now_ms);
  if (rc == TURBO_OK) {
    caller->last_used = flowie_control_auth_rate_next_sequence(limiter);
    identity->last_used = flowie_control_auth_rate_next_sequence(limiter);
    if (caller->available_units < FLOWIE_CONTROL_AUTH_RATE_TOKEN_UNITS ||
        identity->available_units < FLOWIE_CONTROL_AUTH_RATE_TOKEN_UNITS)
      rc = TURBO_EBUSY;
    else {
      caller->available_units -= FLOWIE_CONTROL_AUTH_RATE_TOKEN_UNITS;
      identity->available_units -= FLOWIE_CONTROL_AUTH_RATE_TOKEN_UNITS;
    }
  }
  turbo_mutex_unlock(&limiter->lock);
  flowie_control_credential_wipe(identity_digest, sizeof(identity_digest));
  flowie_control_credential_wipe(caller_digest, sizeof(caller_digest));
  return rc;
}

void flowie_control_auth_rate_limiter_record_success(
    flowie_control_auth_rate_limiter_t *limiter, const char *peer_certificate_sha256,
    const char *root_group_id, const char *principal_id) {
  uint8_t digest[FLOWIE_CONTROL_AUTH_RATE_DIGEST_SIZE] = {0};
  if (!limiter ||
      !flowie_control_auth_rate_text_valid(peer_certificate_sha256,
                                           FLOWIE_CONTROL_AUTH_CERT_SHA256_TEXT_SIZE) ||
      !flowie_control_auth_rate_text_valid(root_group_id, TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_auth_rate_text_valid(principal_id, TURBO_FLOW_SECURITY_ID_MAX))
    return;
  flowie_control_auth_rate_digest(limiter, FLOWIE_CONTROL_AUTH_RATE_IDENTITY_SCOPE,
                                  peer_certificate_sha256, root_group_id, principal_id, digest);
  turbo_mutex_lock(&limiter->lock);
  flowie_control_auth_rate_remove_locked(&limiter->identities, digest);
  turbo_mutex_unlock(&limiter->lock);
  flowie_control_credential_wipe(digest, sizeof(digest));
}

size_t flowie_control_auth_rate_limiter_caller_size(flowie_control_auth_rate_limiter_t *limiter) {
  size_t size;
  if (!limiter) return 0u;
  turbo_mutex_lock(&limiter->lock);
  size = turbo_hash_map_size(&limiter->callers);
  turbo_mutex_unlock(&limiter->lock);
  return size;
}

size_t flowie_control_auth_rate_limiter_identity_size(
    flowie_control_auth_rate_limiter_t *limiter) {
  size_t size;
  if (!limiter) return 0u;
  turbo_mutex_lock(&limiter->lock);
  size = turbo_hash_map_size(&limiter->identities);
  turbo_mutex_unlock(&limiter->lock);
  return size;
}
