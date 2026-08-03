#include "flowie_cluster_topology_internal.h"

#include "turbo_error.h"
#include "turbo_vec.h"

#include <stdlib.h>
#include <string.h>

typedef struct flowie_cluster_topology_owned_operation_s {
  flowie_cluster_topology_operation_kind_t kind;
  tstr_t node_id;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_t advertised_endpoint;
} flowie_cluster_topology_owned_operation_t;

struct flowie_cluster_topology_plan_s {
  uint64_t membership_revision;
  turbo_vec_t operations;
  int operations_initialized;
};

static int flowie_cluster_topology_nonzero(const uint8_t *value, size_t size) {
  uint8_t combined = 0u;
  size_t index;
  if (!value) return 0;
  for (index = 0u; index < size; ++index)
    combined |= value[index];
  return combined != 0u;
}

static int flowie_cluster_topology_text_valid(tstr_v value, size_t maximum) {
  return value.data && value.len != 0u && value.len <= maximum &&
         memchr(value.data, '\0', value.len) == NULL;
}

static int flowie_cluster_topology_text_compare(tstr_v left, tstr_v right) {
  size_t common = left.len < right.len ? left.len : right.len;
  int compared = common == 0u ? 0 : memcmp(left.data, right.data, common);
  if (compared != 0) return compared;
  return left.len < right.len ? -1 : left.len > right.len ? 1 : 0;
}

static int flowie_cluster_topology_text_equal(tstr_v left, tstr_v right) {
  return left.len == right.len && (left.len == 0u || memcmp(left.data, right.data, left.len) == 0);
}

static int flowie_cluster_topology_state_valid(flowie_cluster_node_state_t state) {
  return state >= FLOWIE_CLUSTER_NODE_STARTING && state <= FLOWIE_CLUSTER_NODE_EXPIRED;
}

static int flowie_cluster_topology_state_connectable(flowie_cluster_node_state_t state) {
  return state == FLOWIE_CLUSTER_NODE_SYNCING || state == FLOWIE_CLUSTER_NODE_READY ||
         state == FLOWIE_CLUSTER_NODE_DRAINING;
}

int flowie_cluster_topology_plan_config_validate(
    const flowie_cluster_topology_plan_config_t *config) {
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_TOPOLOGY_ABI_V1 ||
      !flowie_cluster_topology_text_valid(config->local_node_id, FLOWIE_CLUSTER_NODE_ID_MAX) ||
      !flowie_cluster_topology_nonzero(config->local_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
      config->max_nodes == 0u || config->max_nodes > FLOWIE_CLUSTER_NODE_COUNT_MAX ||
      config->max_endpoint_size == 0u ||
      config->max_endpoint_size > FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX)
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_topology_membership_validate(
    const flowie_cluster_topology_plan_config_t *config,
    const flowie_cluster_topology_membership_t *membership) {
  tstr_v previous = {NULL, 0u};
  int local_seen = 0;
  size_t index;
  if (!membership || membership->size != sizeof(*membership) ||
      membership->abi_version != FLOWIE_CLUSTER_TOPOLOGY_ABI_V1 ||
      membership->membership_revision == 0u || membership->member_count > config->max_nodes ||
      (membership->member_count != 0u && !membership->members))
    return TURBO_EINVAL;
  if (membership->membership_revision < config->last_applied_revision) return TURBO_EBUSY;
  for (index = 0u; index < membership->member_count; ++index) {
    const flowie_cluster_topology_member_t *member = &membership->members[index];
    int local_compare;
    if (member->size != sizeof(*member) || member->abi_version != FLOWIE_CLUSTER_TOPOLOGY_ABI_V1 ||
        !flowie_cluster_topology_text_valid(member->node_id, FLOWIE_CLUSTER_NODE_ID_MAX) ||
        !flowie_cluster_topology_nonzero(member->boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
        !flowie_cluster_topology_state_valid(member->state) ||
        !flowie_cluster_topology_text_valid(member->advertised_endpoint,
                                            config->max_endpoint_size) ||
        member->revision == 0u || member->revision > membership->membership_revision ||
        (index != 0u && flowie_cluster_topology_text_compare(previous, member->node_id) >= 0))
      return TURBO_EPROTO;
    previous = member->node_id;
    local_compare = flowie_cluster_topology_text_compare(member->node_id, config->local_node_id);
    if (local_compare == 0 &&
        memcmp(member->boot_id, config->local_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) != 0)
      return TURBO_EBUSY;
    if (local_compare == 0) local_seen = 1;
  }
  return local_seen ? TURBO_OK : TURBO_EBUSY;
}

static int
flowie_cluster_topology_current_validate(const flowie_cluster_topology_plan_config_t *config,
                                         const flowie_cluster_topology_peer_t *current_peers,
                                         size_t current_peer_count) {
  tstr_v previous = {NULL, 0u};
  size_t index;
  if (current_peer_count > config->max_nodes || (current_peer_count != 0u && !current_peers))
    return TURBO_EINVAL;
  for (index = 0u; index < current_peer_count; ++index) {
    const flowie_cluster_topology_peer_t *peer = &current_peers[index];
    if (peer->size != sizeof(*peer) || peer->abi_version != FLOWIE_CLUSTER_TOPOLOGY_ABI_V1 ||
        !flowie_cluster_topology_text_valid(peer->node_id, FLOWIE_CLUSTER_NODE_ID_MAX) ||
        !flowie_cluster_topology_nonzero(peer->boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
        !flowie_cluster_topology_text_valid(peer->advertised_endpoint, config->max_endpoint_size) ||
        flowie_cluster_topology_text_compare(config->local_node_id, peer->node_id) >= 0 ||
        (index != 0u && flowie_cluster_topology_text_compare(previous, peer->node_id) >= 0))
      return TURBO_EPROTO;
    previous = peer->node_id;
  }
  return TURBO_OK;
}

static int
flowie_cluster_topology_member_desired(const flowie_cluster_topology_plan_config_t *config,
                                       const flowie_cluster_topology_member_t *member) {
  return flowie_cluster_topology_state_connectable(member->state) &&
         flowie_cluster_topology_text_compare(config->local_node_id, member->node_id) < 0;
}

static const flowie_cluster_topology_member_t *
flowie_cluster_topology_next_desired(const flowie_cluster_topology_plan_config_t *config,
                                     const flowie_cluster_topology_membership_t *membership,
                                     size_t *index) {
  while (*index < membership->member_count) {
    const flowie_cluster_topology_member_t *member = &membership->members[(*index)++];
    if (flowie_cluster_topology_member_desired(config, member)) return member;
  }
  return NULL;
}

static int
flowie_cluster_topology_peer_matches_member(const flowie_cluster_topology_peer_t *peer,
                                            const flowie_cluster_topology_member_t *member) {
  return peer && member &&
         memcmp(peer->boot_id, member->boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) == 0 &&
         flowie_cluster_topology_text_equal(peer->advertised_endpoint, member->advertised_endpoint);
}

static int flowie_cluster_topology_plan_append(flowie_cluster_topology_plan_t *plan,
                                               flowie_cluster_topology_operation_kind_t kind,
                                               tstr_v node_id,
                                               const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                               tstr_v endpoint) {
  flowie_cluster_topology_owned_operation_t operation;
  int rc;
  memset(&operation, 0, sizeof(operation));
  operation.kind = kind;
  operation.node_id = tstr_from_v(node_id);
  operation.advertised_endpoint = tstr_from_v(endpoint);
  if (!operation.node_id || !operation.advertised_endpoint) {
    tstr_free(operation.node_id);
    tstr_free(operation.advertised_endpoint);
    return TURBO_ENOMEM;
  }
  memcpy(operation.boot_id, boot_id, sizeof(operation.boot_id));
  rc = turbo_vec_push(&plan->operations, &operation);
  if (rc != TURBO_OK) {
    tstr_free(operation.node_id);
    tstr_free(operation.advertised_endpoint);
  }
  return rc;
}

static int flowie_cluster_topology_plan_removals(
    flowie_cluster_topology_plan_t *plan, const flowie_cluster_topology_plan_config_t *config,
    const flowie_cluster_topology_membership_t *membership,
    const flowie_cluster_topology_peer_t *current_peers, size_t current_peer_count) {
  size_t current_index = 0u;
  size_t member_index = 0u;
  const flowie_cluster_topology_member_t *desired =
      flowie_cluster_topology_next_desired(config, membership, &member_index);
  while (current_index < current_peer_count) {
    const flowie_cluster_topology_peer_t *current = &current_peers[current_index];
    int compared =
        desired ? flowie_cluster_topology_text_compare(current->node_id, desired->node_id) : -1;
    if (!desired || compared < 0) {
      int rc = flowie_cluster_topology_plan_append(plan, FLOWIE_CLUSTER_TOPOLOGY_REMOVE,
                                                   current->node_id, current->boot_id,
                                                   current->advertised_endpoint);
      if (rc != TURBO_OK) return rc;
      ++current_index;
    } else if (compared > 0) {
      desired = flowie_cluster_topology_next_desired(config, membership, &member_index);
    } else {
      if (!flowie_cluster_topology_peer_matches_member(current, desired)) {
        int rc = flowie_cluster_topology_plan_append(plan, FLOWIE_CLUSTER_TOPOLOGY_REMOVE,
                                                     current->node_id, current->boot_id,
                                                     current->advertised_endpoint);
        if (rc != TURBO_OK) return rc;
      }
      ++current_index;
      desired = flowie_cluster_topology_next_desired(config, membership, &member_index);
    }
  }
  return TURBO_OK;
}

static int flowie_cluster_topology_plan_additions(
    flowie_cluster_topology_plan_t *plan, const flowie_cluster_topology_plan_config_t *config,
    const flowie_cluster_topology_membership_t *membership,
    const flowie_cluster_topology_peer_t *current_peers, size_t current_peer_count) {
  size_t current_index = 0u;
  size_t member_index = 0u;
  const flowie_cluster_topology_member_t *desired;
  while ((desired = flowie_cluster_topology_next_desired(config, membership, &member_index)) !=
         NULL) {
    int compared;
    while (current_index < current_peer_count &&
           flowie_cluster_topology_text_compare(current_peers[current_index].node_id,
                                                desired->node_id) < 0)
      ++current_index;
    compared = current_index < current_peer_count
                   ? flowie_cluster_topology_text_compare(current_peers[current_index].node_id,
                                                          desired->node_id)
                   : 1;
    if (compared != 0 ||
        !flowie_cluster_topology_peer_matches_member(&current_peers[current_index], desired)) {
      int rc =
          flowie_cluster_topology_plan_append(plan, FLOWIE_CLUSTER_TOPOLOGY_ADD, desired->node_id,
                                              desired->boot_id, desired->advertised_endpoint);
      if (rc != TURBO_OK) return rc;
    }
    if (compared == 0) ++current_index;
  }
  return TURBO_OK;
}

int flowie_cluster_topology_plan_build(const flowie_cluster_topology_plan_config_t *config,
                                       const flowie_cluster_topology_membership_t *membership,
                                       const flowie_cluster_topology_peer_t *current_peers,
                                       size_t current_peer_count,
                                       flowie_cluster_topology_plan_t **out) {
  flowie_cluster_topology_plan_t *plan;
  size_t maximum_operations;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_topology_plan_config_validate(config);
  if (rc == TURBO_OK) rc = flowie_cluster_topology_membership_validate(config, membership);
  if (rc == TURBO_OK)
    rc = flowie_cluster_topology_current_validate(config, current_peers, current_peer_count);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  if (membership->member_count > SIZE_MAX - current_peer_count) return TURBO_ERANGE;
  maximum_operations = membership->member_count + current_peer_count;
  plan = (flowie_cluster_topology_plan_t *)calloc(1u, sizeof(*plan));
  if (!plan) return TURBO_ENOMEM;
  rc = turbo_vec_init(&plan->operations, sizeof(flowie_cluster_topology_owned_operation_t));
  if (rc != TURBO_OK) goto fail;
  plan->operations_initialized = 1;
  if (maximum_operations != 0u) {
    rc = turbo_vec_reserve(&plan->operations, maximum_operations);
    if (rc != TURBO_OK) goto fail;
  }
  plan->membership_revision = membership->membership_revision;
  rc = flowie_cluster_topology_plan_removals(plan, config, membership, current_peers,
                                             current_peer_count);
  if (rc == TURBO_OK)
    rc = flowie_cluster_topology_plan_additions(plan, config, membership, current_peers,
                                                current_peer_count);
  if (rc != TURBO_OK) goto fail;
  if (membership->membership_revision == config->last_applied_revision &&
      turbo_vec_size(&plan->operations) != 0u) {
    rc = TURBO_EPROTO;
    goto fail;
  }
  *out = plan;
  return TURBO_OK;

fail:
  flowie_cluster_topology_plan_destroy(plan);
  return rc;
}

uint64_t flowie_cluster_topology_plan_revision(const flowie_cluster_topology_plan_t *plan) {
  return plan ? plan->membership_revision : 0u;
}

size_t flowie_cluster_topology_plan_operation_count(const flowie_cluster_topology_plan_t *plan) {
  return plan && plan->operations_initialized ? turbo_vec_size(&plan->operations) : 0u;
}

int flowie_cluster_topology_plan_operation_at(const flowie_cluster_topology_plan_t *plan,
                                              size_t index,
                                              flowie_cluster_topology_operation_t *out) {
  const flowie_cluster_topology_owned_operation_t *owned;
  flowie_cluster_topology_operation_t operation = FLOWIE_CLUSTER_TOPOLOGY_OPERATION_INIT;
  if (!plan || !plan->operations_initialized || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_TOPOLOGY_ABI_V1)
    return TURBO_EINVAL;
  owned = (const flowie_cluster_topology_owned_operation_t *)turbo_vec_at_const(&plan->operations,
                                                                                index);
  if (!owned) return TURBO_ENOENT;
  operation.kind = owned->kind;
  operation.peer.node_id = tstr_to_v(owned->node_id);
  memcpy(operation.peer.boot_id, owned->boot_id, sizeof(operation.peer.boot_id));
  operation.peer.advertised_endpoint = tstr_to_v(owned->advertised_endpoint);
  *out = operation;
  return TURBO_OK;
}

void flowie_cluster_topology_plan_destroy(flowie_cluster_topology_plan_t *plan) {
  size_t index;
  if (!plan) return;
  if (plan->operations_initialized) {
    for (index = 0u; index < turbo_vec_size(&plan->operations); ++index) {
      flowie_cluster_topology_owned_operation_t *operation =
          (flowie_cluster_topology_owned_operation_t *)turbo_vec_at(&plan->operations, index);
      if (!operation) continue;
      tstr_free(operation->node_id);
      tstr_free(operation->advertised_endpoint);
    }
    turbo_vec_destroy(&plan->operations);
  }
  free(plan);
}
