#ifndef TURBO_FLOW_CNET_MANAGED_SINK_H
#define TURBO_FLOW_CNET_MANAGED_SINK_H

#include "turbo_flow_cnet.h"
#define XXH_INLINE_ALL
#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <xxhash.h>

typedef struct turbo_flow_cnet_sink_identity_s {
  char owner[TURBO_FLOW_RESOURCE_OWNER_MAX + 1u];
  char uid[TURBO_FLOW_RESOURCE_UID_MAX + 1u];
} turbo_flow_cnet_sink_identity_t;

/* Identity is fixed at registration; long adapter names use the same bounded
 * digest for every transport, independent of runtime generations. */
static inline int turbo_flow_cnet_sink_identity_init(turbo_flow_cnet_sink_identity_t *identity,
                                                     const char *adapter_name,
                                                     const char *uid_prefix) {
  const size_t length = strlen(adapter_name);
  int written;
  if (length <= TURBO_FLOW_RESOURCE_OWNER_MAX) {
    memcpy(identity->owner, adapter_name, length + 1u);
  } else {
    const XXH128_hash_t hash = XXH3_128bits(adapter_name, length);
    written = snprintf(identity->owner, sizeof(identity->owner),
                       "xxh3-128:%016" PRIx64 "%016" PRIx64, hash.high64, hash.low64);
    if (written < 0 || (size_t)written >= sizeof(identity->owner)) return SALTS_ERANGE;
  }
  written = snprintf(identity->uid, sizeof(identity->uid), "%s%s", uid_prefix, identity->owner);
  if (written < 0 || (size_t)written >= sizeof(identity->uid)) return SALTS_ERANGE;
  return SALTS_OK;
}

/* Cumulative settlement counters never wrap across stop/start cycles. */
static inline void turbo_flow_cnet_sink_counter_increment(atomic_uint_fast64_t *counter) {
  uint_fast64_t current = atomic_load_explicit(counter, memory_order_relaxed);
  while (current != UINT64_MAX &&
         !atomic_compare_exchange_weak_explicit(counter, &current, current + 1u,
                                                memory_order_relaxed, memory_order_relaxed)) {
  }
}
#endif
