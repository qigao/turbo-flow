#ifndef FLOWIE_CONTROL_AUTH_RATE_LIMITER_INTERNAL_H
#define FLOWIE_CONTROL_AUTH_RATE_LIMITER_INTERNAL_H

#include "flowie_control_security_limits_internal.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CONTROL_AUTH_RATE_DEFAULT_CALLER_CAPACITY FLOWIE_CONTROL_AUTH_MAX_BINDINGS
#define FLOWIE_CONTROL_AUTH_RATE_DEFAULT_IDENTITY_CAPACITY 4096u
#define FLOWIE_CONTROL_AUTH_RATE_MAX_CAPACITY 65536u
#define FLOWIE_CONTROL_AUTH_RATE_DEFAULT_CALLER_PER_SECOND 100u
#define FLOWIE_CONTROL_AUTH_RATE_DEFAULT_CALLER_BURST 200u
#define FLOWIE_CONTROL_AUTH_RATE_DEFAULT_IDENTITY_PER_SECOND 5u
#define FLOWIE_CONTROL_AUTH_RATE_DEFAULT_IDENTITY_BURST 10u
#define FLOWIE_CONTROL_AUTH_RATE_MAX_PER_SECOND 10000u
#define FLOWIE_CONTROL_AUTH_RATE_MAX_BURST 100000u

typedef struct flowie_control_auth_rate_limiter_s flowie_control_auth_rate_limiter_t;
typedef uint64_t (*flowie_control_auth_rate_clock_fn)(void *ctx);

typedef struct flowie_control_auth_rate_limiter_config_s {
  size_t size;
  size_t caller_capacity;
  size_t identity_capacity;
  uint32_t caller_per_second;
  uint32_t caller_burst;
  uint32_t identity_per_second;
  uint32_t identity_burst;
  flowie_control_auth_rate_clock_fn clock_ms;
  void *clock_ctx;
} flowie_control_auth_rate_limiter_config_t;

#define FLOWIE_CONTROL_AUTH_RATE_LIMITER_CONFIG_INIT                                              \
  {sizeof(flowie_control_auth_rate_limiter_config_t),                                             \
   FLOWIE_CONTROL_AUTH_RATE_DEFAULT_CALLER_CAPACITY,                                              \
   FLOWIE_CONTROL_AUTH_RATE_DEFAULT_IDENTITY_CAPACITY,                                            \
   FLOWIE_CONTROL_AUTH_RATE_DEFAULT_CALLER_PER_SECOND,                                            \
   FLOWIE_CONTROL_AUTH_RATE_DEFAULT_CALLER_BURST,                                                 \
   FLOWIE_CONTROL_AUTH_RATE_DEFAULT_IDENTITY_PER_SECOND,                                          \
   FLOWIE_CONTROL_AUTH_RATE_DEFAULT_IDENTITY_BURST,                                               \
   NULL,                                                                                          \
   NULL}

int flowie_control_auth_rate_limiter_create(
    const flowie_control_auth_rate_limiter_config_t *config,
    flowie_control_auth_rate_limiter_t **out);
void flowie_control_auth_rate_limiter_destroy(flowie_control_auth_rate_limiter_t *limiter);

/** Consume one caller and one identity token atomically, or return TURBO_EBUSY. */
int flowie_control_auth_rate_limiter_acquire(flowie_control_auth_rate_limiter_t *limiter,
                                             const char *peer_certificate_sha256,
                                             const char *domain_id,
                                             const char *principal_id);

/** Remove only the identity failure bucket after successful credential verification. */
void flowie_control_auth_rate_limiter_record_success(
    flowie_control_auth_rate_limiter_t *limiter, const char *peer_certificate_sha256,
    const char *domain_id, const char *principal_id);

size_t flowie_control_auth_rate_limiter_caller_size(flowie_control_auth_rate_limiter_t *limiter);
size_t flowie_control_auth_rate_limiter_identity_size(flowie_control_auth_rate_limiter_t *limiter);

#ifdef __cplusplus
}
#endif

#endif
