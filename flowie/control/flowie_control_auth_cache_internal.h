#ifndef FLOWIE_CONTROL_AUTH_CACHE_INTERNAL_H
#define FLOWIE_CONTROL_AUTH_CACHE_INTERNAL_H

#include "flowie_control_repository_internal.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_AUTH_CACHE_DEFAULT_CAPACITY 256u
#define FLOWIE_CONTROL_AUTH_CACHE_MAX_CAPACITY 4096u
#define FLOWIE_CONTROL_AUTH_CACHE_DEFAULT_TTL_MS 5000u
#define FLOWIE_CONTROL_AUTH_CACHE_MAX_TTL_MS 60000u

typedef struct flowie_control_auth_cache_s flowie_control_auth_cache_t;
typedef uint64_t (*flowie_control_auth_cache_clock_fn)(void *ctx);

typedef struct flowie_control_auth_cache_config_s {
  size_t size;
  size_t capacity;
  uint64_t ttl_ms;
  flowie_control_auth_cache_clock_fn clock_ms;
  void *clock_ctx;
} flowie_control_auth_cache_config_t;

#define FLOWIE_CONTROL_AUTH_CACHE_CONFIG_INIT                                                      \
  {sizeof(flowie_control_auth_cache_config_t), FLOWIE_CONTROL_AUTH_CACHE_DEFAULT_CAPACITY,         \
   FLOWIE_CONTROL_AUTH_CACHE_DEFAULT_TTL_MS, NULL, NULL}

/**
 * Create a thread-safe positive credential cache.
 *
 * The cache owns a process-random digest key and never retains caller credential bytes. The clock
 * callback is borrowed and must remain valid until destroy. Destroy requires no concurrent calls.
 */
int flowie_control_auth_cache_create(const flowie_control_auth_cache_config_t *config,
                                     flowie_control_auth_cache_t **out);

void flowie_control_auth_cache_destroy(flowie_control_auth_cache_t *cache);

/**
 * Verify a credential using a bounded positive/negative cache and the control repository.
 *
 * A cache candidate is accepted only after the current user and credential revisions are read from
 * the store. On a miss or revision change, the store performs the full Argon2id verification.
 * Explicit permission failures are cached for the configured TTL; storage/provider errors are
 * never cached.
 */
int flowie_control_auth_cache_verify(flowie_control_auth_cache_t *cache,
                                     const flowie_control_repository_t *repository,
                                     const char *domain_id, const char *principal_id,
                                     const void *secret, size_t secret_size,
                                     flowie_control_credential_verify_result_t *result,
                                     int *cache_hit_out);

size_t flowie_control_auth_cache_size(flowie_control_auth_cache_t *cache);

#ifdef __cplusplus
}
#endif

#endif
