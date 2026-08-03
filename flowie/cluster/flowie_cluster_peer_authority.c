#include "flowie_cluster_peer_authority_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"
#include "turbo_vec.h"

#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_peer_authority_pin_s {
  char node_id[FLOWIE_CLUSTER_NODE_ID_MAX + 1u];
  size_t node_id_size;
  char certificate_sha256[FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE];
} flowie_cluster_peer_authority_pin_t;

typedef struct flowie_cluster_peer_authority_member_s {
  char node_id[FLOWIE_CLUSTER_NODE_ID_MAX + 1u];
  size_t node_id_size;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  char advertised_endpoint[FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX + 1u];
  size_t advertised_endpoint_size;
  char certificate_sha256[FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE];
} flowie_cluster_peer_authority_member_t;

struct flowie_cluster_peer_authority_s {
  turbo_vec_t pins;
  turbo_vec_t active;
  turbo_vec_t scratch;
  size_t max_peers;
  uint64_t revision;
  turbo_mutex_t mutex;
  int mutex_initialized;
  int pins_initialized;
  int active_initialized;
  int scratch_initialized;
};

static int flowie_cluster_peer_authority_view_compare(tstr_v left, tstr_v right) {
  size_t common = left.len < right.len ? left.len : right.len;
  int order = common == 0u ? 0 : memcmp(left.data, right.data, common);
  if (order != 0) return order;
  return left.len < right.len ? -1 : left.len > right.len ? 1 : 0;
}

static tstr_v flowie_cluster_peer_authority_view(const char *data, size_t size) {
  tstr_v view = {data, size};
  return view;
}

static int flowie_cluster_peer_authority_pin_compare(const void *left, const void *right) {
  const flowie_cluster_peer_authority_pin_t *lhs =
      (const flowie_cluster_peer_authority_pin_t *)left;
  const flowie_cluster_peer_authority_pin_t *rhs =
      (const flowie_cluster_peer_authority_pin_t *)right;
  return strcmp(lhs->node_id, rhs->node_id);
}

static int flowie_cluster_peer_authority_fingerprint_valid(const char *value) {
  size_t index;
  if (!value || strncmp(value, "sha256:", 7u) != 0 ||
      strnlen(value, FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE) !=
          FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE - 1u)
    return 0;
  for (index = 7u; index < FLOWIE_CLUSTER_PEER_CERTIFICATE_SHA256_SIZE - 1u; ++index)
    if (!((value[index] >= '0' && value[index] <= '9') ||
          (value[index] >= 'a' && value[index] <= 'f')))
      return 0;
  return 1;
}

static int flowie_cluster_peer_authority_boot_nonzero(
    const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  uint8_t combined = 0u;
  size_t index;
  if (!boot_id) return 0;
  for (index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    combined |= boot_id[index];
  return combined != 0u;
}

static int flowie_cluster_peer_authority_pin_find(
    const flowie_cluster_peer_authority_t *authority, tstr_v node_id,
    const flowie_cluster_peer_authority_pin_t **out) {
  size_t first = 0u;
  size_t count;
  if (out) *out = NULL;
  if (!authority || !node_id.data || node_id.len == 0u || !out) return TURBO_EINVAL;
  count = turbo_vec_size(&authority->pins);
  while (count != 0u) {
    size_t step = count / 2u;
    size_t index = first + step;
    const flowie_cluster_peer_authority_pin_t *pin =
        (const flowie_cluster_peer_authority_pin_t *)turbo_vec_at_const(&authority->pins, index);
    int order = flowie_cluster_peer_authority_view_compare(
        flowie_cluster_peer_authority_view(pin->node_id, pin->node_id_size), node_id);
    if (order < 0) {
      first = index + 1u;
      count -= step + 1u;
    } else {
      count = step;
    }
  }
  if (first < turbo_vec_size(&authority->pins)) {
    const flowie_cluster_peer_authority_pin_t *pin =
        (const flowie_cluster_peer_authority_pin_t *)turbo_vec_at_const(&authority->pins, first);
    if (flowie_cluster_peer_authority_view_compare(
            flowie_cluster_peer_authority_view(pin->node_id, pin->node_id_size), node_id) == 0) {
      *out = pin;
      return TURBO_OK;
    }
  }
  return TURBO_ENOENT;
}

static void flowie_cluster_peer_authority_free(flowie_cluster_peer_authority_t *authority) {
  if (!authority) return;
  if (authority->scratch_initialized) turbo_vec_destroy(&authority->scratch);
  if (authority->active_initialized) turbo_vec_destroy(&authority->active);
  if (authority->pins_initialized) turbo_vec_destroy(&authority->pins);
  if (authority->mutex_initialized) turbo_mutex_destroy(&authority->mutex);
  free(authority);
}

int flowie_cluster_peer_authority_create(const flowie_cluster_peer_authority_config_t *config,
                                         flowie_cluster_peer_authority_t **out) {
  flowie_cluster_peer_authority_t *authority;
  size_t index;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PEER_AUTHORITY_ABI_V1 || !out ||
      config->max_peers == 0u || config->max_peers > FLOWIE_CLUSTER_NODE_COUNT_MAX ||
      config->pin_count == 0u || config->pin_count > FLOWIE_CLUSTER_NODE_COUNT_MAX || !config->pins)
    return TURBO_EINVAL;
  authority = (flowie_cluster_peer_authority_t *)calloc(1u, sizeof(*authority));
  if (!authority) return TURBO_ENOMEM;
  authority->max_peers = config->max_peers;
  turbo_mutex_init(&authority->mutex);
  authority->mutex_initialized = 1;
  rc = turbo_vec_init(&authority->pins, sizeof(flowie_cluster_peer_authority_pin_t));
  if (rc != TURBO_OK) goto fail;
  authority->pins_initialized = 1;
  rc = turbo_vec_init(&authority->active, sizeof(flowie_cluster_peer_authority_member_t));
  if (rc != TURBO_OK) goto fail;
  authority->active_initialized = 1;
  rc = turbo_vec_init(&authority->scratch, sizeof(flowie_cluster_peer_authority_member_t));
  if (rc != TURBO_OK) goto fail;
  authority->scratch_initialized = 1;
  rc = turbo_vec_reserve(&authority->pins, config->pin_count);
  if (rc == TURBO_OK) rc = turbo_vec_reserve(&authority->active, config->max_peers);
  if (rc == TURBO_OK) rc = turbo_vec_reserve(&authority->scratch, config->max_peers);
  if (rc != TURBO_OK) goto fail;
  for (index = 0u; index < config->pin_count; ++index) {
    const flowie_cluster_peer_certificate_pin_t *source = &config->pins[index];
    flowie_cluster_peer_authority_pin_t pin = {0};
    if (source->size != sizeof(*source) ||
        source->abi_version != FLOWIE_CLUSTER_PEER_AUTHORITY_ABI_V1 || !source->node_id.data ||
        source->node_id.len == 0u || source->node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX ||
        memchr(source->node_id.data, '\0', source->node_id.len) ||
        !flowie_cluster_peer_authority_fingerprint_valid(source->certificate_sha256)) {
      rc = TURBO_EINVAL;
      goto fail;
    }
    memcpy(pin.node_id, source->node_id.data, source->node_id.len);
    pin.node_id_size = source->node_id.len;
    memcpy(pin.certificate_sha256, source->certificate_sha256,
           sizeof(pin.certificate_sha256));
    rc = turbo_vec_push(&authority->pins, &pin);
    if (rc != TURBO_OK) goto fail;
  }
  qsort(turbo_vec_data(&authority->pins), turbo_vec_size(&authority->pins),
        sizeof(flowie_cluster_peer_authority_pin_t), flowie_cluster_peer_authority_pin_compare);
  for (index = 1u; index < turbo_vec_size(&authority->pins); ++index) {
    const flowie_cluster_peer_authority_pin_t *previous =
        (const flowie_cluster_peer_authority_pin_t *)turbo_vec_at_const(&authority->pins,
                                                                        index - 1u);
    const flowie_cluster_peer_authority_pin_t *current =
        (const flowie_cluster_peer_authority_pin_t *)turbo_vec_at_const(&authority->pins, index);
    if (strcmp(previous->node_id, current->node_id) == 0) {
      rc = TURBO_EINVAL;
      goto fail;
    }
  }
  *out = authority;
  return TURBO_OK;

fail:
  flowie_cluster_peer_authority_free(authority);
  return rc;
}

int flowie_cluster_peer_authority_replace(flowie_cluster_peer_authority_t *authority,
                                          const flowie_cluster_topology_peer_t *peers,
                                          size_t peer_count, uint64_t revision) {
  tstr_v previous_id = {NULL, 0u};
  size_t index;
  int rc = TURBO_OK;
  if (!authority || peer_count > authority->max_peers ||
      (peer_count != 0u && !peers))
    return TURBO_EINVAL;
  turbo_vec_clear(&authority->scratch);
  for (index = 0u; index < peer_count; ++index) {
    const flowie_cluster_topology_peer_t *peer = &peers[index];
    const flowie_cluster_peer_authority_pin_t *pin = NULL;
    flowie_cluster_peer_authority_member_t member = {0};
    if (peer->size != sizeof(*peer) || peer->abi_version != FLOWIE_CLUSTER_TOPOLOGY_ABI_V1 ||
        !peer->node_id.data || peer->node_id.len == 0u ||
        peer->node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX ||
        memchr(peer->node_id.data, '\0', peer->node_id.len) || !peer->advertised_endpoint.data ||
        peer->advertised_endpoint.len == 0u ||
        peer->advertised_endpoint.len > FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX ||
        memchr(peer->advertised_endpoint.data, '\0', peer->advertised_endpoint.len) ||
        !flowie_cluster_peer_authority_boot_nonzero(peer->boot_id) ||
        (index != 0u &&
         flowie_cluster_peer_authority_view_compare(previous_id, peer->node_id) >= 0))
      return TURBO_EINVAL;
    rc = flowie_cluster_peer_authority_pin_find(authority, peer->node_id, &pin);
    if (rc != TURBO_OK) return TURBO_EPERM;
    memcpy(member.node_id, peer->node_id.data, peer->node_id.len);
    member.node_id_size = peer->node_id.len;
    memcpy(member.boot_id, peer->boot_id, sizeof(member.boot_id));
    memcpy(member.advertised_endpoint, peer->advertised_endpoint.data,
           peer->advertised_endpoint.len);
    member.advertised_endpoint_size = peer->advertised_endpoint.len;
    memcpy(member.certificate_sha256, pin->certificate_sha256,
           sizeof(member.certificate_sha256));
    rc = turbo_vec_push(&authority->scratch, &member);
    if (rc != TURBO_OK) return rc;
    previous_id = peer->node_id;
  }
  turbo_mutex_lock(&authority->mutex);
  {
    turbo_vec_t old = authority->active;
    authority->active = authority->scratch;
    authority->scratch = old;
    authority->revision = revision;
  }
  turbo_mutex_unlock(&authority->mutex);
  turbo_vec_clear(&authority->scratch);
  return TURBO_OK;
}

int flowie_cluster_peer_authority_snapshot(flowie_cluster_peer_authority_t *authority,
                                           flowie_cluster_topology_peer_t *storage,
                                           size_t capacity, size_t *out_count,
                                           uint64_t *out_revision) {
  size_t count;
  size_t index;
  if (out_count) *out_count = 0u;
  if (out_revision) *out_revision = 0u;
  if (!authority || !out_count || !out_revision) return TURBO_EINVAL;
  turbo_mutex_lock(&authority->mutex);
  count = turbo_vec_size(&authority->active);
  if (capacity < count || (count != 0u && !storage)) {
    turbo_mutex_unlock(&authority->mutex);
    return capacity < count ? TURBO_ENOSPC : TURBO_EINVAL;
  }
  for (index = 0u; index < count; ++index) {
    const flowie_cluster_peer_authority_member_t *member =
        (const flowie_cluster_peer_authority_member_t *)turbo_vec_at_const(&authority->active,
                                                                           index);
    flowie_cluster_topology_peer_t peer = FLOWIE_CLUSTER_TOPOLOGY_PEER_INIT;
    peer.node_id = flowie_cluster_peer_authority_view(member->node_id, member->node_id_size);
    memcpy(peer.boot_id, member->boot_id, sizeof(peer.boot_id));
    peer.advertised_endpoint =
        flowie_cluster_peer_authority_view(member->advertised_endpoint,
                                           member->advertised_endpoint_size);
    storage[index] = peer;
  }
  *out_count = count;
  *out_revision = authority->revision;
  turbo_mutex_unlock(&authority->mutex);
  return TURBO_OK;
}

int flowie_cluster_peer_authority_authorize(
    void *ctx, tstr_v peer_node_id,
    const uint8_t peer_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    const char *peer_certificate_sha256) {
  flowie_cluster_peer_authority_t *authority = (flowie_cluster_peer_authority_t *)ctx;
  size_t first = 0u;
  size_t count;
  int rc = TURBO_EPERM;
  if (!authority || !peer_node_id.data || peer_node_id.len == 0u ||
      !peer_boot_id || !peer_certificate_sha256)
    return TURBO_EINVAL;
  turbo_mutex_lock(&authority->mutex);
  count = turbo_vec_size(&authority->active);
  while (count != 0u) {
    size_t step = count / 2u;
    size_t index = first + step;
    const flowie_cluster_peer_authority_member_t *member =
        (const flowie_cluster_peer_authority_member_t *)turbo_vec_at_const(&authority->active,
                                                                           index);
    int order = flowie_cluster_peer_authority_view_compare(
        flowie_cluster_peer_authority_view(member->node_id, member->node_id_size), peer_node_id);
    if (order < 0) {
      first = index + 1u;
      count -= step + 1u;
    } else {
      count = step;
    }
  }
  if (first < turbo_vec_size(&authority->active)) {
    const flowie_cluster_peer_authority_member_t *member =
        (const flowie_cluster_peer_authority_member_t *)turbo_vec_at_const(&authority->active,
                                                                           first);
    if (flowie_cluster_peer_authority_view_compare(
            flowie_cluster_peer_authority_view(member->node_id, member->node_id_size),
                                               peer_node_id) == 0 &&
        memcmp(member->boot_id, peer_boot_id, sizeof(member->boot_id)) == 0 &&
        strcmp(member->certificate_sha256, peer_certificate_sha256) == 0)
      rc = TURBO_OK;
  }
  turbo_mutex_unlock(&authority->mutex);
  return rc;
}

void flowie_cluster_peer_authority_destroy(flowie_cluster_peer_authority_t *authority) {
  flowie_cluster_peer_authority_free(authority);
}
