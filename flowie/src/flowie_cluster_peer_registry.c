#include "flowie_cluster_peer_internal.h"

#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

typedef enum flowie_cluster_peer_registry_entry_state_e {
  FLOWIE_CLUSTER_PEER_REGISTRY_ENTRY_FREE = 0,
  FLOWIE_CLUSTER_PEER_REGISTRY_ENTRY_ACTIVE,
  FLOWIE_CLUSTER_PEER_REGISTRY_ENTRY_DRAINING
} flowie_cluster_peer_registry_entry_state_t;

typedef struct flowie_cluster_peer_registry_entry_s {
  tstr_t remote_node_id;
  uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  flowie_cluster_peer_link_t *link;
  size_t inflight_sends;
  flowie_cluster_peer_registry_entry_state_t state;
} flowie_cluster_peer_registry_entry_t;

typedef struct flowie_cluster_peer_registry_completion_s {
  struct flowie_cluster_peer_registry_s *registry;
  flowie_cluster_peer_registry_entry_t *entry;
  flowie_cluster_peer_send_complete_fn complete;
  void *complete_ctx;
} flowie_cluster_peer_registry_completion_t;

struct flowie_cluster_peer_registry_s {
  flowie_cluster_peer_registry_entry_t *entries;
  size_t max_links;
  size_t max_inflight_sends;
  size_t registered_links;
  size_t inflight_sends;
  int closing;
  int drained;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
};

static int flowie_cluster_peer_registry_nonzero(const uint8_t *value, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (value[index] != 0u) return 1;
  return 0;
}

static int flowie_cluster_peer_registry_identity_validate(
    tstr_v remote_node_id, const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  if (!remote_node_id.data || remote_node_id.len == 0u ||
      remote_node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX ||
      memchr(remote_node_id.data, '\0', remote_node_id.len) || !remote_boot_id ||
      !flowie_cluster_peer_registry_nonzero(remote_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE))
    return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_peer_registry_identity_equal(
    const flowie_cluster_peer_registry_entry_t *entry, tstr_v remote_node_id,
    const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  return entry->state != FLOWIE_CLUSTER_PEER_REGISTRY_ENTRY_FREE &&
         tstr_len(entry->remote_node_id) == remote_node_id.len &&
         memcmp(entry->remote_node_id, remote_node_id.data, remote_node_id.len) == 0 &&
         memcmp(entry->remote_boot_id, remote_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) == 0;
}

static flowie_cluster_peer_registry_entry_t *
flowie_cluster_peer_registry_find(flowie_cluster_peer_registry_t *registry, tstr_v remote_node_id,
                                  const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  size_t index;
  for (index = 0u; index < registry->max_links; ++index)
    if (flowie_cluster_peer_registry_identity_equal(&registry->entries[index], remote_node_id,
                                                    remote_boot_id))
      return &registry->entries[index];
  return NULL;
}

static flowie_cluster_peer_registry_entry_t *
flowie_cluster_peer_registry_free_entry(flowie_cluster_peer_registry_t *registry) {
  size_t index;
  for (index = 0u; index < registry->max_links; ++index)
    if (registry->entries[index].state == FLOWIE_CLUSTER_PEER_REGISTRY_ENTRY_FREE)
      return &registry->entries[index];
  return NULL;
}

int flowie_cluster_peer_registry_create(const flowie_cluster_peer_registry_config_t *config,
                                        flowie_cluster_peer_registry_t **out) {
  flowie_cluster_peer_registry_t *registry;
  if (out) *out = NULL;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PEER_REGISTRY_ABI_V1 || config->max_links == 0u ||
      config->max_links > SIZE_MAX / sizeof(flowie_cluster_peer_registry_entry_t) ||
      config->max_inflight_sends == 0u || !out)
    return TURBO_EINVAL;
  registry = (flowie_cluster_peer_registry_t *)calloc(1u, sizeof(*registry));
  if (!registry) return TURBO_ENOMEM;
  registry->entries = (flowie_cluster_peer_registry_entry_t *)calloc(
      config->max_links, sizeof(flowie_cluster_peer_registry_entry_t));
  if (!registry->entries) {
    free(registry);
    return TURBO_ENOMEM;
  }
  registry->max_links = config->max_links;
  registry->max_inflight_sends = config->max_inflight_sends;
  turbo_mutex_init(&registry->mutex);
  turbo_cond_init(&registry->changed);
  *out = registry;
  return TURBO_OK;
}

int flowie_cluster_peer_registry_register(flowie_cluster_peer_registry_t *registry,
                                          tstr_v remote_node_id,
                                          const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE],
                                          flowie_cluster_peer_link_t *link) {
  flowie_cluster_peer_registry_entry_t *entry;
  tstr_t owned_node_id;
  int rc;
  if (!registry || !link) return TURBO_EINVAL;
  rc = flowie_cluster_peer_registry_identity_validate(remote_node_id, remote_boot_id);
  if (rc != TURBO_OK) return rc;
  owned_node_id = tstr_from_v(remote_node_id);
  if (!owned_node_id) return TURBO_ENOMEM;
  turbo_mutex_lock(&registry->mutex);
  entry = flowie_cluster_peer_registry_find(registry, remote_node_id, remote_boot_id);
  if (registry->closing) rc = TURBO_ESHUTDOWN;
  else if (entry) rc = entry->link == link ? TURBO_EALREADY : TURBO_EBUSY;
  else if (registry->registered_links == registry->max_links ||
           !(entry = flowie_cluster_peer_registry_free_entry(registry)))
    rc = TURBO_ENOSPC;
  else {
    entry->remote_node_id = owned_node_id;
    owned_node_id = NULL;
    memcpy(entry->remote_boot_id, remote_boot_id, sizeof(entry->remote_boot_id));
    entry->link = link;
    entry->state = FLOWIE_CLUSTER_PEER_REGISTRY_ENTRY_ACTIVE;
    ++registry->registered_links;
    registry->drained = 0;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&registry->mutex);
  tstr_free(owned_node_id);
  return rc;
}

int flowie_cluster_peer_registry_unregister(
    flowie_cluster_peer_registry_t *registry, tstr_v remote_node_id,
    const uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], flowie_cluster_peer_link_t *link) {
  flowie_cluster_peer_registry_entry_t *entry;
  tstr_t owned_node_id = NULL;
  int rc;
  if (!registry || !link) return TURBO_EINVAL;
  rc = flowie_cluster_peer_registry_identity_validate(remote_node_id, remote_boot_id);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&registry->mutex);
  entry = flowie_cluster_peer_registry_find(registry, remote_node_id, remote_boot_id);
  if (!entry) rc = TURBO_ENOENT;
  else if (entry->link != link) rc = TURBO_EBUSY;
  else if (entry->inflight_sends != 0u) {
    entry->state = FLOWIE_CLUSTER_PEER_REGISTRY_ENTRY_DRAINING;
    rc = TURBO_EBUSY;
  } else {
    owned_node_id = entry->remote_node_id;
    memset(entry, 0, sizeof(*entry));
    --registry->registered_links;
    turbo_cond_broadcast(&registry->changed);
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&registry->mutex);
  tstr_free(owned_node_id);
  return rc;
}

static void flowie_cluster_peer_registry_complete(void *ctx, int status) {
  flowie_cluster_peer_registry_completion_t *completion =
      (flowie_cluster_peer_registry_completion_t *)ctx;
  flowie_cluster_peer_send_complete_fn complete = completion->complete;
  void *complete_ctx = completion->complete_ctx;
  flowie_cluster_peer_registry_t *registry = completion->registry;
  if (complete) complete(complete_ctx, status);
  turbo_mutex_lock(&registry->mutex);
  if (completion->entry->inflight_sends != 0u) --completion->entry->inflight_sends;
  if (registry->inflight_sends != 0u) --registry->inflight_sends;
  if (registry->closing && registry->inflight_sends == 0u) registry->drained = 1;
  turbo_cond_broadcast(&registry->changed);
  turbo_mutex_unlock(&registry->mutex);
  free(completion);
}

int flowie_cluster_peer_registry_send(void *ctx, const flowie_cluster_peer_frame_t *frame,
                                      flowie_cluster_peer_send_complete_fn complete,
                                      void *complete_ctx) {
  flowie_cluster_peer_registry_t *registry = (flowie_cluster_peer_registry_t *)ctx;
  flowie_cluster_peer_registry_completion_t *completion;
  flowie_cluster_peer_registry_entry_t *entry;
  int rc;
  if (!registry || !frame) return TURBO_EINVAL;
  rc = flowie_cluster_peer_registry_identity_validate(frame->target_node_id, frame->target_boot_id);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&registry->mutex);
  entry = flowie_cluster_peer_registry_find(registry, frame->target_node_id, frame->target_boot_id);
  if (registry->closing) rc = TURBO_ESHUTDOWN;
  else if (!entry || entry->state != FLOWIE_CLUSTER_PEER_REGISTRY_ENTRY_ACTIVE) rc = TURBO_ENOENT;
  else if (registry->inflight_sends >= registry->max_inflight_sends) rc = TURBO_ENOSPC;
  else {
    ++entry->inflight_sends;
    ++registry->inflight_sends;
    registry->drained = 0;
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&registry->mutex);
  if (rc != TURBO_OK) return rc;
  completion = (flowie_cluster_peer_registry_completion_t *)calloc(1u, sizeof(*completion));
  if (!completion) {
    turbo_mutex_lock(&registry->mutex);
    --entry->inflight_sends;
    --registry->inflight_sends;
    turbo_cond_broadcast(&registry->changed);
    turbo_mutex_unlock(&registry->mutex);
    return TURBO_ENOMEM;
  }
  completion->registry = registry;
  completion->entry = entry;
  completion->complete = complete;
  completion->complete_ctx = complete_ctx;
  rc = flowie_cluster_peer_link_send(entry->link, frame, flowie_cluster_peer_registry_complete,
                                     completion);
  if (rc != TURBO_OK) {
    turbo_mutex_lock(&registry->mutex);
    --entry->inflight_sends;
    --registry->inflight_sends;
    turbo_cond_broadcast(&registry->changed);
    turbo_mutex_unlock(&registry->mutex);
    free(completion);
  }
  return rc;
}

int flowie_cluster_peer_registry_snapshot(flowie_cluster_peer_registry_t *registry,
                                          flowie_cluster_peer_registry_snapshot_t *out) {
  size_t index;
  if (!registry || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PEER_REGISTRY_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&registry->mutex);
  out->registered_links = registry->registered_links;
  out->draining_links = 0u;
  for (index = 0u; index < registry->max_links; ++index)
    if (registry->entries[index].state == FLOWIE_CLUSTER_PEER_REGISTRY_ENTRY_DRAINING)
      ++out->draining_links;
  out->inflight_sends = registry->inflight_sends;
  out->closing = registry->closing;
  turbo_mutex_unlock(&registry->mutex);
  return TURBO_OK;
}

int flowie_cluster_peer_registry_close(flowie_cluster_peer_registry_t *registry) {
  int rc = TURBO_OK;
  if (!registry) return TURBO_EINVAL;
  turbo_mutex_lock(&registry->mutex);
  if (registry->closing) rc = TURBO_EALREADY;
  else {
    registry->closing = 1;
    registry->drained = registry->inflight_sends == 0u;
    turbo_cond_broadcast(&registry->changed);
  }
  turbo_mutex_unlock(&registry->mutex);
  return rc;
}

int flowie_cluster_peer_registry_drain(flowie_cluster_peer_registry_t *registry,
                                       uint64_t timeout_ns) {
  uint64_t start_ns;
  uint64_t deadline_ns;
  int rc = TURBO_OK;
  if (!registry) return TURBO_EINVAL;
  start_ns = turbo_hrtime();
  deadline_ns = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + timeout_ns;
  turbo_mutex_lock(&registry->mutex);
  if (!registry->closing) rc = TURBO_EBUSY;
  while (rc == TURBO_OK && registry->inflight_sends != 0u) {
    uint64_t now_ns;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&registry->changed, &registry->mutex);
      continue;
    }
    now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) {
      rc = timeout_ns == 0u ? TURBO_EBUSY : TURBO_ETIMEDOUT;
      break;
    }
    (void)turbo_cond_timedwait(&registry->changed, &registry->mutex, deadline_ns - now_ns);
  }
  if (rc == TURBO_OK) registry->drained = 1;
  turbo_mutex_unlock(&registry->mutex);
  return rc;
}

int flowie_cluster_peer_registry_destroy(flowie_cluster_peer_registry_t *registry) {
  int ready;
  if (!registry) return TURBO_EINVAL;
  turbo_mutex_lock(&registry->mutex);
  ready = registry->closing && registry->drained && registry->inflight_sends == 0u &&
          registry->registered_links == 0u;
  turbo_mutex_unlock(&registry->mutex);
  if (!ready) return TURBO_EBUSY;
  turbo_cond_destroy(&registry->changed);
  turbo_mutex_destroy(&registry->mutex);
  free(registry->entries);
  free(registry);
  return TURBO_OK;
}
