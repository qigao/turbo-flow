#include "flowie_cluster_member_directory_internal.h"

#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

struct flowie_cluster_member_directory_snapshot_s {
  flowie_cluster_pgsql_member_t *members;
  size_t member_count;
  uint64_t membership_revision;
};

struct flowie_cluster_member_directory_s {
  size_t max_members;
  flowie_cluster_member_directory_snapshot_t *published;
  turbo_rwlock_t lock;
};

static int flowie_cluster_member_directory_member_validate(
    const flowie_cluster_pgsql_member_t *member, uint64_t membership_revision,
    const flowie_cluster_pgsql_member_t *previous) {
  if (!member || member->size != sizeof(*member) ||
      member->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || member->node_id_size == 0u ||
      member->node_id_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      member->node_id[member->node_id_size] != '\0' ||
      member->advertised_endpoint_size == 0u ||
      member->advertised_endpoint_size > FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX ||
      member->advertised_endpoint[member->advertised_endpoint_size] != '\0' ||
      member->state < FLOWIE_CLUSTER_NODE_STARTING ||
      member->state > FLOWIE_CLUSTER_NODE_EXPIRED || member->lease_deadline_epoch_ms == 0u ||
      member->revision == 0u || member->revision > membership_revision ||
      (previous && strcmp(previous->node_id, member->node_id) >= 0))
    return TURBO_EPROTO;
  return TURBO_OK;
}

int flowie_cluster_member_directory_create(size_t max_members,
                                           flowie_cluster_member_directory_t **out) {
  flowie_cluster_member_directory_t *directory;
  if (out) *out = NULL;
  if (!out || max_members == 0u || max_members > FLOWIE_CLUSTER_NODE_COUNT_MAX)
    return TURBO_EINVAL;
  directory = (flowie_cluster_member_directory_t *)calloc(1u, sizeof(*directory));
  if (!directory) return TURBO_ENOMEM;
  if (turbo_rwlock_init(&directory->lock) != TURBO_OK) {
    free(directory);
    return TURBO_EIO;
  }
  directory->max_members = max_members;
  *out = directory;
  return TURBO_OK;
}

int flowie_cluster_member_directory_prepare(
    const flowie_cluster_member_directory_t *directory,
    const flowie_cluster_pgsql_membership_snapshot_t *source,
    flowie_cluster_member_directory_snapshot_t **out) {
  flowie_cluster_member_directory_snapshot_t *candidate;
  if (out) *out = NULL;
  if (!directory || !source || !out || source->size != sizeof(*source) ||
      source->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 ||
      source->member_count > directory->max_members ||
      (source->membership_revision == 0u && source->member_count != 0u) ||
      (source->member_count != 0u && !source->members))
    return TURBO_EINVAL;
  candidate = (flowie_cluster_member_directory_snapshot_t *)calloc(1u, sizeof(*candidate));
  if (!candidate) return TURBO_ENOMEM;
  if (source->member_count != 0u) {
    if (source->member_count > SIZE_MAX / sizeof(*candidate->members)) {
      free(candidate);
      return TURBO_ERANGE;
    }
    candidate->members = (flowie_cluster_pgsql_member_t *)calloc(
        source->member_count, sizeof(*candidate->members));
    if (!candidate->members) {
      free(candidate);
      return TURBO_ENOMEM;
    }
  }
  for (size_t index = 0u; index < source->member_count; ++index) {
    int rc = flowie_cluster_member_directory_member_validate(
        &source->members[index], source->membership_revision,
        index == 0u ? NULL : &source->members[index - 1u]);
    if (rc != TURBO_OK) {
      flowie_cluster_member_directory_snapshot_destroy(candidate);
      return rc;
    }
  }
  if (source->member_count != 0u)
    memcpy(candidate->members, source->members,
           source->member_count * sizeof(*candidate->members));
  candidate->member_count = source->member_count;
  candidate->membership_revision = source->membership_revision;
  *out = candidate;
  return TURBO_OK;
}

int flowie_cluster_member_directory_publish(
    flowie_cluster_member_directory_t *directory,
    flowie_cluster_member_directory_snapshot_t *candidate) {
  flowie_cluster_member_directory_snapshot_t *previous;
  if (!directory || !candidate || candidate->member_count > directory->max_members ||
      (candidate->membership_revision == 0u && candidate->member_count != 0u))
    return TURBO_EINVAL;
  turbo_rwlock_wrlock(&directory->lock);
  if (directory->published &&
      candidate->membership_revision < directory->published->membership_revision) {
    turbo_rwlock_wrunlock(&directory->lock);
    return TURBO_EBUSY;
  }
  previous = directory->published;
  directory->published = candidate;
  turbo_rwlock_wrunlock(&directory->lock);
  flowie_cluster_member_directory_snapshot_destroy(previous);
  return TURBO_OK;
}

void flowie_cluster_member_directory_snapshot_destroy(
    flowie_cluster_member_directory_snapshot_t *snapshot) {
  if (!snapshot) return;
  free(snapshot->members);
  free(snapshot);
}

static size_t flowie_cluster_member_directory_lower_bound(
    const flowie_cluster_member_directory_snapshot_t *snapshot, tstr_v node_id, int *found) {
  size_t first = 0u;
  size_t count = snapshot ? snapshot->member_count : 0u;
  while (count != 0u) {
    size_t step = count / 2u;
    size_t index = first + step;
    const flowie_cluster_pgsql_member_t *member = &snapshot->members[index];
    size_t common = member->node_id_size < node_id.len ? member->node_id_size : node_id.len;
    int order = common == 0u ? 0 : memcmp(member->node_id, node_id.data, common);
    if (order == 0)
      order = member->node_id_size < node_id.len ? -1 : member->node_id_size > node_id.len ? 1 : 0;
    if (order < 0) {
      first = index + 1u;
      count -= step + 1u;
    } else {
      count = step;
    }
  }
  if (found) {
    const flowie_cluster_pgsql_member_t *member =
        snapshot && first < snapshot->member_count ? &snapshot->members[first] : NULL;
    *found = member && member->node_id_size == node_id.len &&
             memcmp(member->node_id, node_id.data, node_id.len) == 0;
  }
  return first;
}

int flowie_cluster_member_directory_resolve(
    void *ctx, tstr_v node_id, const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
    flowie_cluster_pgsql_member_t *out) {
  flowie_cluster_member_directory_t *directory = (flowie_cluster_member_directory_t *)ctx;
  size_t index;
  int found = 0;
  int rc = TURBO_ENOENT;
  if (!directory || !node_id.data || node_id.len == 0u ||
      node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX || !boot_id || !out ||
      out->size != sizeof(*out) || out->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1)
    return TURBO_EINVAL;
  turbo_rwlock_rdlock(&directory->lock);
  index = flowie_cluster_member_directory_lower_bound(directory->published, node_id, &found);
  if (found) {
    const flowie_cluster_pgsql_member_t *member = &directory->published->members[index];
    if (memcmp(member->boot_id, boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) == 0) {
      *out = *member;
      rc = TURBO_OK;
    }
  }
  turbo_rwlock_rdunlock(&directory->lock);
  return rc;
}

void flowie_cluster_member_directory_destroy(flowie_cluster_member_directory_t *directory) {
  if (!directory) return;
  flowie_cluster_member_directory_snapshot_destroy(directory->published);
  turbo_rwlock_destroy(&directory->lock);
  free(directory);
}
