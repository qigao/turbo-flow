#ifndef FLOWIE_CLUSTER_PEER_AUTHORITY_INTERNAL_H
#define FLOWIE_CLUSTER_PEER_AUTHORITY_INTERNAL_H

#include "flowie_cluster_peer_internal.h"
#include "flowie_cluster_topology_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FLOWIE_CLUSTER_PEER_AUTHORITY_ABI_V1 1u
#define FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE 72u

/** Stable certificate pin for one node identity. */
typedef struct flowie_cluster_peer_certificate_pin_s {
  size_t size;
  uint32_t abi_version;
  tstr_v node_id;
  /** Canonical `sha256:` plus 64 lowercase hexadecimal digits. */
  const char *certificate_sha256;
} flowie_cluster_peer_certificate_pin_t;

#define FLOWIE_CLUSTER_PEER_CERTIFICATE_PIN_INIT                                                  \
  {sizeof(flowie_cluster_peer_certificate_pin_t), FLOWIE_CLUSTER_PEER_AUTHORITY_ABI_V1,           \
   {NULL, 0u}, NULL}

typedef struct flowie_cluster_peer_authority_config_s {
  size_t size;
  uint32_t abi_version;
  size_t max_peers;
  const flowie_cluster_peer_certificate_pin_t *pins;
  size_t pin_count;
} flowie_cluster_peer_authority_config_t;

#define FLOWIE_CLUSTER_PEER_AUTHORITY_CONFIG_INIT                                                 \
  {sizeof(flowie_cluster_peer_authority_config_t), FLOWIE_CLUSTER_PEER_AUTHORITY_ABI_V1, 0u, NULL, \
   0u}

typedef struct flowie_cluster_peer_authority_s flowie_cluster_peer_authority_t;

/** Copy and validate a complete bounded node-to-certificate map. */
int flowie_cluster_peer_authority_create(const flowie_cluster_peer_authority_config_t *config,
                                         flowie_cluster_peer_authority_t **out);

/**
 * Atomically replace the PostgreSQL-derived active peer set. Input peers must
 * be strictly node-id ordered and every peer must have one configured pin.
 */
int flowie_cluster_peer_authority_replace(flowie_cluster_peer_authority_t *authority,
                                          const flowie_cluster_topology_peer_t *peers,
                                          size_t peer_count, uint64_t revision);

/**
 * Single-topology-writer snapshot. The structs are copied into caller storage,
 * while their string views borrow authority storage and remain valid only until
 * the next replace or destroy. Authorization calls may run concurrently.
 */
int flowie_cluster_peer_authority_snapshot(flowie_cluster_peer_authority_t *authority,
                                           flowie_cluster_topology_peer_t *storage,
                                           size_t capacity, size_t *out_count,
                                           uint64_t *out_revision);

/** Peer transport authorization callback: certificate + node + boot must all match. */
int flowie_cluster_peer_authority_authorize(
    void *ctx, tstr_v peer_node_id,
    const uint8_t peer_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    const char *peer_certificate_sha256);

void flowie_cluster_peer_authority_destroy(flowie_cluster_peer_authority_t *authority);

#ifdef __cplusplus
}
#endif

#endif /* FLOWIE_CLUSTER_PEER_AUTHORITY_INTERNAL_H */
