#include "flowie_control_management_session_internal.h"

#include "flowie_control_credential_internal.h"

#include "monocypher.h"
#include "platform.h"
#include "turbo_error.h"
#include "turbo_hash.h"
#include "turbo_thread.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE 32u
#define FLOWIE_CONTROL_MANAGEMENT_SESSION_KEY_SIZE 32u
#define FLOWIE_CONTROL_MANAGEMENT_SESSION_RANDOM_SIZE 32u
#define FLOWIE_CONTROL_MANAGEMENT_SESSION_SCOPE "management-session"
#define FLOWIE_CONTROL_MANAGEMENT_SESSION_MAX_CAPACITY 65536u
#define FLOWIE_CONTROL_MANAGEMENT_SESSION_MAX_PER_PRINCIPAL 65536u
#define FLOWIE_CONTROL_MANAGEMENT_SESSION_MAX_TTL_SECONDS 86400u

typedef struct flowie_control_management_session_entry_s {
  char root_group_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char csrf[FLOWIE_CONTROL_MANAGEMENT_SESSION_CSRF_SIZE + 1u];
  uint64_t expires_at;
  uint64_t issued_sequence;
  uint64_t last_used;
} flowie_control_management_session_entry_t;

struct flowie_control_management_session_store_s {
  flowie_control_repository_t repository;
  flowie_control_auth_service_t *auth_service;
  turbo_hash_map_t sessions;
  turbo_mutex_t lock;
  uint8_t digest_key[FLOWIE_CONTROL_MANAGEMENT_SESSION_KEY_SIZE];
  char method[TURBO_FLOW_SECURITY_TYPE_MAX + 1u];
  size_t capacity;
  size_t max_sessions_per_principal;
  uint64_t ttl_seconds;
  uint64_t access_sequence;
  flowie_control_management_session_clock_fn clock;
  void *clock_ctx;
};


static uint64_t flowie_control_management_session_default_clock(void *ctx) {
  (void)ctx;
  return turbo_realtime_ms() / 1000u;
}

static int flowie_control_management_session_text_valid(const char *value, size_t maximum) {
  size_t size;
  if (!value || maximum == 0u) return 0;
  size = strnlen(value, maximum + 1u);
  if (size == 0u || size > maximum) return 0;
  for (size_t index = 0u; index < size; ++index) {
    unsigned char byte = (unsigned char)value[index];
    if (byte < 0x20u || byte == 0x7fu) return 0;
  }
  return 1;
}

static void flowie_control_management_session_hex(
    const uint8_t input[FLOWIE_CONTROL_MANAGEMENT_SESSION_RANDOM_SIZE],
    char output[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]) {
  static const char hex[] = "0123456789abcdef";
  for (size_t index = 0u; index < FLOWIE_CONTROL_MANAGEMENT_SESSION_RANDOM_SIZE; ++index) {
    output[index * 2u] = hex[input[index] >> 4u];
    output[index * 2u + 1u] = hex[input[index] & 0x0fu];
  }
  output[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE] = '\0';
}

static int flowie_control_management_session_token_valid(const char *token) {
  if (!token ||
      strnlen(token, FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u) !=
          FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE)
    return 0;
  for (size_t index = 0u; index < FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE; ++index) {
    const char byte = token[index];
    if (!((byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f'))) return 0;
  }
  return 1;
}

static void flowie_control_management_session_digest(
    const flowie_control_management_session_store_t *store, const char *token,
    uint8_t digest[FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE]) {
  crypto_blake2b_keyed(digest, FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE, store->digest_key,
                       sizeof(store->digest_key), (const uint8_t *)token,
                       FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE);
}

static uint64_t
flowie_control_management_session_next_sequence(flowie_control_management_session_store_t *store) {
  if (store->access_sequence == UINT64_MAX) {
    uint64_t minimum = UINT64_MAX;
    size_t capacity = turbo_hash_map_capacity(&store->sessions);
    for (size_t slot = 0u; slot < capacity; ++slot) {
      flowie_control_management_session_entry_t *entry =
          (flowie_control_management_session_entry_t *)turbo_hash_map_value_at(&store->sessions,
                                                                               slot);
      if (entry && entry->issued_sequence < minimum) minimum = entry->issued_sequence;
    }
    if (minimum == UINT64_MAX) {
      store->access_sequence = 0u;
    } else {
      for (size_t slot = 0u; slot < capacity; ++slot) {
        flowie_control_management_session_entry_t *entry =
            (flowie_control_management_session_entry_t *)turbo_hash_map_value_at(&store->sessions,
                                                                                 slot);
        if (entry) {
          entry->issued_sequence -= minimum;
          entry->last_used -= minimum;
        }
      }
      store->access_sequence -= minimum;
    }
  }
  return ++store->access_sequence;
}

static void flowie_control_management_session_remove_locked(
    flowie_control_management_session_store_t *store,
    const uint8_t digest[FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE]) {
  flowie_control_management_session_entry_t removed = {0};
  if (turbo_hash_map_remove(&store->sessions, digest, &removed) == TURBO_OK)
    flowie_control_credential_wipe(&removed, sizeof(removed));
}

static void flowie_control_management_session_prune_locked(
    flowie_control_management_session_store_t *store, uint64_t now) {
  size_t slot = 0u;
  while (slot < turbo_hash_map_capacity(&store->sessions)) {
    const uint8_t *key = (const uint8_t *)turbo_hash_map_key_at(&store->sessions, slot);
    const flowie_control_management_session_entry_t *entry =
        (const flowie_control_management_session_entry_t *)turbo_hash_map_value_at_const(
            &store->sessions, slot);
    if (key && entry && now >= entry->expires_at) {
      uint8_t digest[FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE];
      memcpy(digest, key, sizeof(digest));
      flowie_control_management_session_remove_locked(store, digest);
      flowie_control_credential_wipe(digest, sizeof(digest));
      continue;
    }
    ++slot;
  }
}

static int flowie_control_management_session_evict_locked(
    flowie_control_management_session_store_t *store) {
  uint8_t oldest[FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE] = {0};
  uint64_t sequence = UINT64_MAX;
  int found = 0;
  size_t capacity = turbo_hash_map_capacity(&store->sessions);
  for (size_t slot = 0u; slot < capacity; ++slot) {
    const uint8_t *key = (const uint8_t *)turbo_hash_map_key_at(&store->sessions, slot);
    const flowie_control_management_session_entry_t *entry =
        (const flowie_control_management_session_entry_t *)turbo_hash_map_value_at_const(
            &store->sessions, slot);
    if (key && entry && (!found || entry->last_used < sequence)) {
      memcpy(oldest, key, sizeof(oldest));
      sequence = entry->last_used;
      found = 1;
    }
  }
  if (found) flowie_control_management_session_remove_locked(store, oldest);
  flowie_control_credential_wipe(oldest, sizeof(oldest));
  return found;
}

static void flowie_control_management_session_evict_principal_locked(
    flowie_control_management_session_store_t *store, const char *root_group_id,
    const char *principal_id) {
  uint8_t oldest[FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE] = {0};
  uint64_t sequence = UINT64_MAX;
  size_t matching = 0u;
  int found = 0;
  size_t capacity = turbo_hash_map_capacity(&store->sessions);
  for (size_t slot = 0u; slot < capacity; ++slot) {
    const uint8_t *key = (const uint8_t *)turbo_hash_map_key_at(&store->sessions, slot);
    const flowie_control_management_session_entry_t *entry =
        (const flowie_control_management_session_entry_t *)turbo_hash_map_value_at_const(
            &store->sessions, slot);
    if (!key || !entry || strcmp(entry->root_group_id, root_group_id) != 0 ||
        strcmp(entry->principal_id, principal_id) != 0)
      continue;
    ++matching;
    if (!found || entry->issued_sequence < sequence) {
      memcpy(oldest, key, sizeof(oldest));
      sequence = entry->issued_sequence;
      found = 1;
    }
  }
  if (matching >= store->max_sessions_per_principal && found)
    flowie_control_management_session_remove_locked(store, oldest);
  flowie_control_credential_wipe(oldest, sizeof(oldest));
}

int flowie_control_management_session_store_create(
    const flowie_control_management_session_config_t *config,
    flowie_control_management_session_store_t **out) {
  flowie_control_management_session_store_t *store = NULL;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      flowie_control_repository_validate(config->repository) != TURBO_OK ||
      !config->auth_service ||
      !flowie_control_management_session_text_valid(config->method,
                                                     TURBO_FLOW_SECURITY_TYPE_MAX) ||
      config->capacity == 0u ||
      config->capacity > FLOWIE_CONTROL_MANAGEMENT_SESSION_MAX_CAPACITY ||
      config->max_sessions_per_principal == 0u ||
      config->max_sessions_per_principal > FLOWIE_CONTROL_MANAGEMENT_SESSION_MAX_PER_PRINCIPAL ||
      config->ttl_seconds < 60u ||
      config->ttl_seconds > FLOWIE_CONTROL_MANAGEMENT_SESSION_MAX_TTL_SECONDS || !out)
    return TURBO_EINVAL;
  store = (flowie_control_management_session_store_t *)calloc(1u, sizeof(*store));
  if (!store) return TURBO_ENOMEM;
  store->repository = *config->repository;
  store->auth_service = config->auth_service;
  store->capacity = config->capacity;
  store->max_sessions_per_principal = config->max_sessions_per_principal;
  store->ttl_seconds = config->ttl_seconds;
  store->clock =
      config->clock ? config->clock : flowie_control_management_session_default_clock;
  store->clock_ctx = config->clock_ctx;
  memcpy(store->method, config->method, strlen(config->method) + 1u);
  rc = turbo_secure_random(store->digest_key, sizeof(store->digest_key));
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_init(&store->sessions, FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE,
                           sizeof(flowie_control_management_session_entry_t), NULL, NULL, NULL);
  if (rc != TURBO_OK) goto fail;
  rc = turbo_hash_map_reserve(&store->sessions, store->capacity);
  if (rc != TURBO_OK) goto fail;
  turbo_mutex_init(&store->lock);
  *out = store;
  return TURBO_OK;

fail:
  turbo_hash_map_destroy(&store->sessions);
  flowie_control_credential_wipe(store, sizeof(*store));
  free(store);
  return rc;
}

void flowie_control_management_session_store_destroy(
    flowie_control_management_session_store_t *store) {
  if (!store) return;
  turbo_mutex_lock(&store->lock);
  for (size_t slot = 0u; slot < turbo_hash_map_capacity(&store->sessions); ++slot) {
    flowie_control_management_session_entry_t *entry =
        (flowie_control_management_session_entry_t *)turbo_hash_map_value_at(&store->sessions,
                                                                             slot);
    if (entry) flowie_control_credential_wipe(entry, sizeof(*entry));
  }
  turbo_hash_map_clear(&store->sessions);
  turbo_mutex_unlock(&store->lock);
  turbo_mutex_destroy(&store->lock);
  turbo_hash_map_destroy(&store->sessions);
  flowie_control_credential_wipe(store, sizeof(*store));
  free(store);
}

static int flowie_control_management_session_authenticate(
    flowie_control_management_session_store_t *store, const char *root_group_id,
    const char *presented_identity, const uint8_t *secret, size_t secret_size,
    const char *remote_address, flowie_control_management_caller_t *caller_out,
    char caller_root_group_out[TURBO_FLOW_SECURITY_ID_MAX + 1u],
    char caller_actor_out[TURBO_FLOW_SECURITY_ID_MAX + 1u],
    uint64_t *principal_expires_at_out) {
  flowie_control_authenticate_request_t request = FLOWIE_CONTROL_AUTHENTICATE_REQUEST_INIT;
  turbo_flow_security_principal_t principal = TURBO_FLOW_SECURITY_PRINCIPAL_INIT;
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  int rc;
  if (caller_out && caller_out->size >= sizeof(*caller_out))
    *caller_out = (flowie_control_management_caller_t)FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  if (caller_root_group_out) caller_root_group_out[0] = '\0';
  if (caller_actor_out) caller_actor_out[0] = '\0';
  if (principal_expires_at_out) *principal_expires_at_out = 0u;
  if (!store || !caller_out || caller_out->size < sizeof(*caller_out) ||
      !caller_root_group_out || !caller_actor_out || !principal_expires_at_out ||
      !flowie_control_management_session_text_valid(root_group_id,
                                                     TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_management_session_text_valid(presented_identity,
                                                     TURBO_FLOW_SECURITY_ID_MAX) ||
      !secret || secret_size == 0u || secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX ||
      !flowie_control_management_session_text_valid(remote_address,
                                                     FLOWIE_CONTROL_AUTH_REMOTE_ADDRESS_MAX))
    return TURBO_EINVAL;
  request.identity = presented_identity;
  request.method = store->method;
  request.secret = secret;
  request.secret_size = secret_size;
  request.protocol = "https";
  request.remote_address = remote_address;
  rc = flowie_control_auth_service_authenticate_root(
      store->auth_service, root_group_id, FLOWIE_CONTROL_MANAGEMENT_SESSION_SCOPE, &request, 0,
      &principal, NULL);
  if (rc != TURBO_OK) goto done;
  rc = flowie_control_management_identity_resolve_principal(
      &store->repository, principal.root_group_id, principal.principal_id, &caller);
  if (rc == TURBO_OK) {
    memcpy(caller_root_group_out, caller.root_group_id, strlen(caller.root_group_id) + 1u);
    memcpy(caller_actor_out, caller.actor, strlen(caller.actor) + 1u);
    caller_out->root_group_id = caller_root_group_out;
    caller_out->actor = caller_actor_out;
    caller_out->permissions = caller.permissions;
    *principal_expires_at_out = principal.expires_at;
  }
done:
  flowie_control_credential_wipe(&principal, sizeof(principal));
  return rc;
}

static int flowie_control_management_session_issue(
    flowie_control_management_session_store_t *store,
    const flowie_control_management_caller_t *caller, uint64_t principal_expires_at,
    char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]) {
  flowie_control_management_session_entry_t entry = {0};
  uint8_t token_random[FLOWIE_CONTROL_MANAGEMENT_SESSION_RANDOM_SIZE] = {0};
  uint8_t csrf_random[FLOWIE_CONTROL_MANAGEMENT_SESSION_RANDOM_SIZE] = {0};
  uint8_t digest[FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE] = {0};
  uint64_t now;
  int rc;
  if (token_out) token_out[0] = '\0';
  if (!store || !caller || caller->size < sizeof(*caller) || !token_out ||
      !flowie_control_management_session_text_valid(caller->root_group_id,
                                                     TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_management_session_text_valid(caller->actor,
                                                     TURBO_FLOW_SECURITY_ID_MAX) ||
      caller->permissions == 0u || principal_expires_at == 0u)
    return TURBO_EINVAL;
  now = store->clock(store->clock_ctx);
  if (now == 0u || now > UINT64_MAX - store->ttl_seconds) {
    rc = TURBO_EIO;
    goto done;
  }
  if (principal_expires_at <= now) {
    rc = TURBO_EPERM;
    goto done;
  }
  rc = turbo_secure_random(token_random, sizeof(token_random));
  if (rc == TURBO_OK) rc = turbo_secure_random(csrf_random, sizeof(csrf_random));
  if (rc != TURBO_OK) goto done;
  flowie_control_management_session_hex(token_random, token_out);
  flowie_control_management_session_hex(csrf_random, entry.csrf);
  memcpy(entry.root_group_id, caller->root_group_id, strlen(caller->root_group_id) + 1u);
  memcpy(entry.principal_id, caller->actor, strlen(caller->actor) + 1u);
  entry.expires_at = now + store->ttl_seconds;
  if (principal_expires_at < entry.expires_at) entry.expires_at = principal_expires_at;
  flowie_control_management_session_digest(store, token_out, digest);
  turbo_mutex_lock(&store->lock);
  flowie_control_management_session_prune_locked(store, now);
  flowie_control_management_session_evict_principal_locked(
      store, entry.root_group_id, entry.principal_id);
  if (turbo_hash_map_size(&store->sessions) >= store->capacity &&
      !flowie_control_management_session_evict_locked(store))
    rc = TURBO_EBUSY;
  if (rc == TURBO_OK) {
    entry.issued_sequence = flowie_control_management_session_next_sequence(store);
    entry.last_used = entry.issued_sequence;
    rc = turbo_hash_map_put(&store->sessions, digest, &entry);
  }
  turbo_mutex_unlock(&store->lock);

done:
  if (rc != TURBO_OK && token_out) token_out[0] = '\0';
  flowie_control_credential_wipe(&entry, sizeof(entry));
  flowie_control_credential_wipe(token_random, sizeof(token_random));
  flowie_control_credential_wipe(csrf_random, sizeof(csrf_random));
  flowie_control_credential_wipe(digest, sizeof(digest));
  return rc;
}

int flowie_control_management_session_login(
    flowie_control_management_session_store_t *store, const char *root_group_id,
    const char *presented_identity, const uint8_t *secret, size_t secret_size,
    const char *remote_address,
    char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]) {
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  char caller_root_group[TURBO_FLOW_SECURITY_ID_MAX + 1u] = {0};
  char caller_actor[TURBO_FLOW_SECURITY_ID_MAX + 1u] = {0};
  uint64_t principal_expires_at = 0u;
  int rc;
  if (token_out) token_out[0] = '\0';
  if (!store || !token_out ||
      !flowie_control_management_session_text_valid(root_group_id,
                                                     TURBO_FLOW_SECURITY_ID_MAX) ||
      !flowie_control_management_session_text_valid(presented_identity,
                                                     TURBO_FLOW_SECURITY_ID_MAX) ||
      !secret || secret_size == 0u || secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX ||
      !flowie_control_management_session_text_valid(remote_address,
                                                     FLOWIE_CONTROL_AUTH_REMOTE_ADDRESS_MAX))
    return TURBO_EINVAL;
  rc = flowie_control_management_session_authenticate(
      store, root_group_id, presented_identity, secret, secret_size, remote_address, &caller,
      caller_root_group, caller_actor, &principal_expires_at);
  if (rc == TURBO_OK)
    rc = flowie_control_management_session_issue(store, &caller, principal_expires_at, token_out);
  flowie_control_credential_wipe(&caller, sizeof(caller));
  flowie_control_credential_wipe(caller_root_group, sizeof(caller_root_group));
  flowie_control_credential_wipe(caller_actor, sizeof(caller_actor));
  return rc;
}

int flowie_control_management_session_resolve(
    flowie_control_management_session_store_t *store, const char *token,
    flowie_control_management_session_identity_t *identity_out) {
  flowie_control_management_session_entry_t entry = {0};
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_management_session_identity_t identity =
      FLOWIE_CONTROL_MANAGEMENT_SESSION_IDENTITY_INIT;
  uint8_t digest[FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE] = {0};
  uint64_t now;
  int found = 0;
  int rc;
  if (identity_out && identity_out->size >= sizeof(*identity_out)) *identity_out = identity;
  if (!store || !flowie_control_management_session_token_valid(token) || !identity_out ||
      identity_out->size < sizeof(*identity_out))
    return TURBO_EPERM;
  now = store->clock(store->clock_ctx);
  if (now == 0u) return TURBO_EIO;
  flowie_control_management_session_digest(store, token, digest);
  turbo_mutex_lock(&store->lock);
  flowie_control_management_session_entry_t *stored =
      (flowie_control_management_session_entry_t *)turbo_hash_map_get(&store->sessions, digest);
  if (stored && now < stored->expires_at) {
    stored->last_used = flowie_control_management_session_next_sequence(store);
    entry = *stored;
    found = 1;
  } else if (stored) {
    flowie_control_management_session_remove_locked(store, digest);
  }
  turbo_mutex_unlock(&store->lock);
  if (!found) {
    rc = TURBO_EPERM;
    goto done;
  }
  rc = flowie_control_management_identity_resolve_principal(
      &store->repository, entry.root_group_id, entry.principal_id, &caller);
  if (rc != TURBO_OK) {
    (void)flowie_control_management_session_revoke(store, token);
    goto done;
  }
  memcpy(identity.root_group_id, caller.root_group_id, strlen(caller.root_group_id) + 1u);
  memcpy(identity.principal_id, caller.actor, strlen(caller.actor) + 1u);
  identity.permissions = caller.permissions;
  memcpy(identity.csrf, entry.csrf, sizeof(entry.csrf));
  *identity_out = identity;

done:
  flowie_control_credential_wipe(&entry, sizeof(entry));
  flowie_control_credential_wipe(digest, sizeof(digest));
  return rc;
}

int flowie_control_management_session_revoke(
    flowie_control_management_session_store_t *store, const char *token) {
  uint8_t digest[FLOWIE_CONTROL_MANAGEMENT_SESSION_DIGEST_SIZE] = {0};
  int rc = TURBO_ENOENT;
  if (!store || !flowie_control_management_session_token_valid(token)) return TURBO_EPERM;
  flowie_control_management_session_digest(store, token, digest);
  turbo_mutex_lock(&store->lock);
  if (turbo_hash_map_get(&store->sessions, digest)) {
    flowie_control_management_session_remove_locked(store, digest);
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&store->lock);
  flowie_control_credential_wipe(digest, sizeof(digest));
  return rc;
}
