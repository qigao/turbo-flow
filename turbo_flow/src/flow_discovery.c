#include "turbo_flow_discovery.h"

#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_flow_stl_error_internal.h"

#include <stdlib.h>
#include <string.h>

typedef struct flow_discovery_slot_s {
  tstr adapter_name;
  tstr peer_id;
  tstr host;
  tstr path;
  int port;
  int active;
} flow_discovery_slot_t;

typedef enum flow_discovery_action_e {
  FLOW_DISCOVERY_ACTION_NONE = 0,
  FLOW_DISCOVERY_ACTION_ADD,
  FLOW_DISCOVERY_ACTION_REPLACE,
  FLOW_DISCOVERY_ACTION_REMOVE
} flow_discovery_action_t;

typedef struct flow_discovery_target_s {
  int assigned;
  tstr peer_id;
  tstr host;
  tstr path;
  int port;
  flow_discovery_action_t action;
} flow_discovery_target_t;

struct turbo_flow_discovery_controller_s {
  turbo_flow_t *flow;
  vec_t slots;
  uint64_t registry_version;
  int consistent;
};

static void flow_discovery_slot_cleanup(flow_discovery_slot_t *slot) {
  if (!slot) return;
  tstr_freep(&slot->adapter_name);
  tstr_freep(&slot->peer_id);
  tstr_freep(&slot->host);
  tstr_freep(&slot->path);
  memset(slot, 0, sizeof(*slot));
}

static void flow_discovery_target_cleanup(flow_discovery_target_t *target) {
  if (!target) return;
  tstr_freep(&target->peer_id);
  tstr_freep(&target->host);
  tstr_freep(&target->path);
  memset(target, 0, sizeof(*target));
}

static int flow_discovery_command(turbo_flow_discovery_controller_t *controller,
                                  const flow_discovery_slot_t *slot,
                                  turbo_flow_adapter_command_kind_t kind,
                                  const flow_discovery_target_t *target) {
  turbo_flow_adapter_command_t command;
  memset(&command, 0, sizeof(command));
  command.size = sizeof(command);
  command.kind = kind;
  if (kind == TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT) {
    if (!target) return TURBO_EINVAL;
    command.endpoint.host = target->host;
    command.endpoint.path = target->path;
    command.endpoint.port = target->port;
  }
  return turbo_flow_adapter_command(controller->flow, slot->adapter_name, &command);
}

static int flow_discovery_peer_valid(const turbo_flow_discovery_peer_t *peer) {
  int has_host;
  int has_path;
  if (!peer || peer->size < sizeof(*peer) || !memchr(peer->peer_id, '\0', sizeof(peer->peer_id)) ||
      !memchr(peer->host, '\0', sizeof(peer->host)) ||
      !memchr(peer->path, '\0', sizeof(peer->path)) || peer->peer_id[0] == '\0') {
    return 0;
  }
  has_host = peer->host[0] != '\0';
  has_path = peer->path[0] != '\0';
  if (has_host) return peer->port > 0 && peer->port <= 65535;
  return has_path && peer->port == 0;
}

static int flow_discovery_slot_endpoint_equal(const flow_discovery_slot_t *slot,
                                              const flow_discovery_target_t *target) {
  return slot && target && slot->active && slot->port == target->port && slot->peer_id &&
         slot->host && slot->path && strcmp(slot->peer_id, target->peer_id) == 0 &&
         strcmp(slot->host, target->host) == 0 && strcmp(slot->path, target->path) == 0;
}

static int flow_discovery_find_active_peer(const turbo_flow_discovery_controller_t *controller,
                                           const char *peer_id) {
  for (size_t i = 0; i < vec_size(&controller->slots); ++i) {
    const flow_discovery_slot_t *slot =
        (const flow_discovery_slot_t *)vec_at_const(&controller->slots, i);
    if (slot && slot->active && slot->peer_id && strcmp(slot->peer_id, peer_id) == 0) {
      return (int)i;
    }
  }
  return -1;
}

static int flow_discovery_target_set(flow_discovery_target_t *target,
                                     const turbo_flow_discovery_peer_t *peer) {
  if (!target || target->assigned) return TURBO_EINVAL;
  target->peer_id = tstr_dup(peer->peer_id);
  target->host = tstr_dup(peer->host);
  target->path = tstr_dup(peer->path);
  if (!target->peer_id || !target->host || !target->path) {
    flow_discovery_target_cleanup(target);
    return TURBO_ENOMEM;
  }
  target->port = peer->port;
  target->assigned = 1;
  return TURBO_OK;
}

static int flow_discovery_build_targets(turbo_flow_discovery_controller_t *controller,
                                        const turbo_flow_discovery_peer_list_t *peer_list,
                                        vec_t *targets) {
  size_t slot_count = vec_size(&controller->slots);
  int rc;
  if (!peer_list || peer_list->size < sizeof(*peer_list) || peer_list->registry_version == 0u ||
      peer_list->peer_count > slot_count ||
      peer_list->peer_count > TURBO_FLOW_DISCOVERY_MAX_PEERS ||
      (peer_list->peer_count > 0u && !peer_list->peers)) {
    return peer_list && peer_list->peer_count > slot_count ? TURBO_ENOSPC : TURBO_EINVAL;
  }
  if (turbo_flow_stl_error(vec_init_bytes(targets, sizeof(flow_discovery_target_t),
                                          _Alignof(flow_discovery_target_t), SIZE_MAX)) != TURBO_OK ||
      turbo_flow_stl_error(vec_resize(targets, slot_count)) != TURBO_OK) {
    vec_destroy(targets);
    return TURBO_ENOMEM;
  }
  for (size_t i = 0; i < peer_list->peer_count; ++i) {
    const turbo_flow_discovery_peer_t *peer = &peer_list->peers[i];
    int slot_index;
    if (!flow_discovery_peer_valid(peer)) return TURBO_EINVAL;
    for (size_t prior = 0; prior < i; ++prior) {
      if (strcmp(peer_list->peers[prior].peer_id, peer->peer_id) == 0) return TURBO_EPROTO;
    }
    slot_index = flow_discovery_find_active_peer(controller, peer->peer_id);
    if (slot_index >= 0) {
      rc = flow_discovery_target_set(
          (flow_discovery_target_t *)vec_at(targets, (size_t)slot_index), peer);
      if (rc != TURBO_OK) return rc;
    }
  }
  for (size_t i = 0; i < peer_list->peer_count; ++i) {
    const turbo_flow_discovery_peer_t *peer = &peer_list->peers[i];
    int existing = flow_discovery_find_active_peer(controller, peer->peer_id);
    size_t selected = slot_count;
    if (existing >= 0) continue;
    for (size_t slot_index = 0; slot_index < slot_count; ++slot_index) {
      flow_discovery_slot_t *slot =
          (flow_discovery_slot_t *)vec_at(&controller->slots, slot_index);
      flow_discovery_target_t *target =
          (flow_discovery_target_t *)vec_at(targets, slot_index);
      if (slot && target && !slot->active && !target->assigned) {
        selected = slot_index;
        break;
      }
    }
    if (selected == slot_count) {
      for (size_t slot_index = 0; slot_index < slot_count; ++slot_index) {
        flow_discovery_target_t *target =
            (flow_discovery_target_t *)vec_at(targets, slot_index);
        if (target && !target->assigned) {
          selected = slot_index;
          break;
        }
      }
    }
    if (selected == slot_count) return TURBO_ENOSPC;
    rc = flow_discovery_target_set((flow_discovery_target_t *)vec_at(targets, selected), peer);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static void flow_discovery_targets_cleanup(vec_t *targets) {
  if (!targets) return;
  for (size_t i = 0; i < vec_size(targets); ++i) {
    flow_discovery_target_cleanup((flow_discovery_target_t *)vec_at(targets, i));
  }
  vec_destroy(targets);
}

static int flow_discovery_targets_equal(const turbo_flow_discovery_controller_t *controller,
                                        const vec_t *targets) {
  for (size_t i = 0; i < vec_size(&controller->slots); ++i) {
    const flow_discovery_slot_t *slot =
        (const flow_discovery_slot_t *)vec_at_const(&controller->slots, i);
    const flow_discovery_target_t *target =
        (const flow_discovery_target_t *)vec_at_const(targets, i);
    if (!slot || !target || slot->active != target->assigned) return 0;
    if (slot->active && !flow_discovery_slot_endpoint_equal(slot, target)) return 0;
  }
  return 1;
}

static int flow_discovery_rollback(turbo_flow_discovery_controller_t *controller,
                                   vec_t *targets) {
  int first_error = TURBO_OK;

  /* Remove commands are applied after every add/replace command. Undo them first. */
  for (size_t i = vec_size(targets); i > 0u; --i) {
    flow_discovery_target_t *target =
        (flow_discovery_target_t *)vec_at(targets, i - 1u);
    flow_discovery_slot_t *slot =
        (flow_discovery_slot_t *)vec_at(&controller->slots, i - 1u);
    int rc = TURBO_OK;
    if (!target || !slot || target->action != FLOW_DISCOVERY_ACTION_REMOVE) continue;
    rc = flow_discovery_command(controller, slot, TURBO_FLOW_ADAPTER_RESUME, NULL);
    if (rc != TURBO_OK && first_error == TURBO_OK) first_error = rc;
  }

  for (size_t i = vec_size(targets); i > 0u; --i) {
    flow_discovery_target_t *target =
        (flow_discovery_target_t *)vec_at(targets, i - 1u);
    flow_discovery_slot_t *slot =
        (flow_discovery_slot_t *)vec_at(&controller->slots, i - 1u);
    int rc = TURBO_OK;
    if (!target || !slot) continue;
    switch (target->action) {
    case FLOW_DISCOVERY_ACTION_ADD:
      rc = flow_discovery_command(controller, slot, TURBO_FLOW_ADAPTER_QUIESCE, NULL);
      break;
    case FLOW_DISCOVERY_ACTION_REPLACE: {
      flow_discovery_target_t old_target;
      memset(&old_target, 0, sizeof(old_target));
      old_target.host = slot->host;
      old_target.path = slot->path;
      old_target.port = slot->port;
      rc = flow_discovery_command(controller, slot, TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT,
                                  &old_target);
      break;
    }
    default:
      break;
    }
    if (rc != TURBO_OK && first_error == TURBO_OK) first_error = rc;
  }
  return first_error;
}

static void flow_discovery_commit(turbo_flow_discovery_controller_t *controller,
                                  vec_t *targets,
                                  turbo_flow_discovery_replace_result_t *result) {
  for (size_t i = 0; i < vec_size(&controller->slots); ++i) {
    flow_discovery_slot_t *slot = (flow_discovery_slot_t *)vec_at(&controller->slots, i);
    flow_discovery_target_t *target = (flow_discovery_target_t *)vec_at(targets, i);
    int unchanged;
    if (!slot || !target) continue;
    unchanged = target->assigned && flow_discovery_slot_endpoint_equal(slot, target);
    if (!target->assigned) {
      if (slot->active) result->removed += 1u;
      tstr_freep(&slot->peer_id);
      tstr_freep(&slot->host);
      tstr_freep(&slot->path);
      slot->port = 0;
      slot->active = 0;
      continue;
    }
    if (!slot->active) result->added += 1u;
    else if (unchanged) result->unchanged += 1u;
    else result->updated += 1u;
    tstr_freep(&slot->peer_id);
    tstr_freep(&slot->host);
    tstr_freep(&slot->path);
    slot->peer_id = target->peer_id;
    slot->host = target->host;
    slot->path = target->path;
    target->peer_id = NULL;
    target->host = NULL;
    target->path = NULL;
    slot->port = target->port;
    slot->active = 1;
  }
}

int turbo_flow_discovery_controller_create(const turbo_flow_discovery_controller_config_t *config,
                                           turbo_flow_discovery_controller_t **out) {
  turbo_flow_discovery_controller_t *controller;
  size_t quiesced = 0u;
  int rc = TURBO_OK;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !out || !config->flow ||
      !config->adapter_names || config->adapter_count == 0u ||
      config->adapter_count > TURBO_FLOW_DISCOVERY_MAX_PEERS ||
      turbo_flow_state(config->flow) != TURBO_FLOW_STATE_STARTED) {
    return TURBO_EINVAL;
  }
  controller = (turbo_flow_discovery_controller_t *)calloc(1, sizeof(*controller));
  if (!controller) return TURBO_ENOMEM;
  if (turbo_flow_stl_error(vec_init_bytes(&controller->slots, sizeof(flow_discovery_slot_t),
                                          _Alignof(flow_discovery_slot_t), SIZE_MAX)) != TURBO_OK ||
      turbo_flow_stl_error(vec_reserve(&controller->slots, config->adapter_count)) != TURBO_OK) {
    vec_destroy(&controller->slots);
    free(controller);
    return TURBO_ENOMEM;
  }
  controller->flow = config->flow;
  controller->consistent = 1;
  for (size_t i = 0; i < config->adapter_count; ++i) {
    flow_discovery_slot_t slot;
    if (!config->adapter_names[i] || !config->adapter_names[i][0]) {
      rc = TURBO_EINVAL;
      break;
    }
    for (size_t prior = 0; prior < i; ++prior) {
      if (strcmp(config->adapter_names[prior], config->adapter_names[i]) == 0) {
        rc = TURBO_EPROTO;
        break;
      }
    }
    if (rc != TURBO_OK) break;
    memset(&slot, 0, sizeof(slot));
    slot.adapter_name = tstr_dup(config->adapter_names[i]);
    if (!slot.adapter_name ||
        turbo_flow_stl_error(vec_push(&controller->slots, &slot)) != TURBO_OK) {
      tstr_freep(&slot.adapter_name);
      rc = TURBO_ENOMEM;
      break;
    }
  }
  if (rc == TURBO_OK) {
    for (; quiesced < vec_size(&controller->slots); ++quiesced) {
      flow_discovery_slot_t *slot =
          (flow_discovery_slot_t *)vec_at(&controller->slots, quiesced);
      rc = flow_discovery_command(controller, slot, TURBO_FLOW_ADAPTER_QUIESCE, NULL);
      if (rc != TURBO_OK) break;
    }
  }
  if (rc != TURBO_OK) {
    while (quiesced > 0u) {
      flow_discovery_slot_t *slot =
          (flow_discovery_slot_t *)vec_at(&controller->slots, --quiesced);
      (void)flow_discovery_command(controller, slot, TURBO_FLOW_ADAPTER_RESUME, NULL);
    }
    turbo_flow_discovery_controller_destroy(controller);
    return rc;
  }
  *out = controller;
  return TURBO_OK;
}

void turbo_flow_discovery_controller_destroy(turbo_flow_discovery_controller_t *controller) {
  if (!controller) return;
  for (size_t i = 0; i < vec_size(&controller->slots); ++i) {
    flow_discovery_slot_cleanup((flow_discovery_slot_t *)vec_at(&controller->slots, i));
  }
  vec_destroy(&controller->slots);
  free(controller);
}

int turbo_flow_discovery_replace_peer_list(turbo_flow_discovery_controller_t *controller,
                                           const turbo_flow_discovery_peer_list_t *peer_list,
                                           turbo_flow_discovery_replace_result_t *result) {
  vec_t targets = {0};
  int rc;
  int rollback_rc = TURBO_OK;
  if (!controller || !controller->consistent || !result || result->size < sizeof(*result)) {
    return TURBO_EINVAL;
  }
  *result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
  result->version_before = controller->registry_version;
  result->version_after = controller->registry_version;
  rc = flow_discovery_build_targets(controller, peer_list, &targets);
  if (rc != TURBO_OK) goto done;
  if (peer_list->registry_version < controller->registry_version) {
    rc = TURBO_EALREADY;
    goto done;
  }
  if (peer_list->registry_version == controller->registry_version) {
    rc = flow_discovery_targets_equal(controller, &targets) ? TURBO_OK : TURBO_EPROTO;
    if (rc == TURBO_OK) result->unchanged = peer_list->peer_count;
    goto done;
  }
  for (size_t i = 0; i < vec_size(&targets); ++i) {
    flow_discovery_slot_t *slot = (flow_discovery_slot_t *)vec_at(&controller->slots, i);
    flow_discovery_target_t *target = (flow_discovery_target_t *)vec_at(&targets, i);
    if (!slot || !target || !target->assigned || flow_discovery_slot_endpoint_equal(slot, target))
      continue;
    rc = flow_discovery_command(controller, slot, TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT, target);
    if (rc != TURBO_OK) goto rollback;
    target->action = slot->active ? FLOW_DISCOVERY_ACTION_REPLACE : FLOW_DISCOVERY_ACTION_ADD;
    if (!slot->active) {
      rc = flow_discovery_command(controller, slot, TURBO_FLOW_ADAPTER_RESUME, NULL);
      if (rc != TURBO_OK) goto rollback;
    }
  }
  for (size_t i = 0; i < vec_size(&targets); ++i) {
    flow_discovery_slot_t *slot = (flow_discovery_slot_t *)vec_at(&controller->slots, i);
    flow_discovery_target_t *target = (flow_discovery_target_t *)vec_at(&targets, i);
    if (!slot || !target || target->assigned || !slot->active) continue;
    rc = flow_discovery_command(controller, slot, TURBO_FLOW_ADAPTER_QUIESCE, NULL);
    if (rc != TURBO_OK) goto rollback;
    target->action = FLOW_DISCOVERY_ACTION_REMOVE;
  }
  flow_discovery_commit(controller, &targets, result);
  controller->registry_version = peer_list->registry_version;
  result->version_after = controller->registry_version;
  rc = TURBO_OK;
  goto done;

rollback:
  rollback_rc = flow_discovery_rollback(controller, &targets);
  if (rollback_rc != TURBO_OK) controller->consistent = 0;

done:
  result->status = rc;
  result->rollback_status = rollback_rc;
  flow_discovery_targets_cleanup(&targets);
  return rollback_rc == TURBO_OK ? rc : rollback_rc;
}

int turbo_flow_discovery_poll(turbo_flow_discovery_controller_t *controller,
                              const turbo_flow_discovery_source_t *source,
                              turbo_flow_discovery_replace_result_t *result) {
  vec_t peers = {0};
  turbo_flow_discovery_peer_list_t peer_list = TURBO_FLOW_DISCOVERY_PEER_LIST_INIT;
  size_t capacity;
  size_t count = 0u;
  uint64_t version = 0u;
  int rc;
  if (!controller || !source || source->size < sizeof(*source) || !source->fetch || !result ||
      result->size < sizeof(*result)) {
    return TURBO_EINVAL;
  }
  *result = (turbo_flow_discovery_replace_result_t)TURBO_FLOW_DISCOVERY_REPLACE_RESULT_INIT;
  result->version_before = controller->registry_version;
  result->version_after = controller->registry_version;
  capacity = vec_size(&controller->slots);
  if (turbo_flow_stl_error(vec_init_bytes(&peers, sizeof(turbo_flow_discovery_peer_t),
                                          _Alignof(turbo_flow_discovery_peer_t), SIZE_MAX)) != TURBO_OK ||
      turbo_flow_stl_error(vec_resize(&peers, capacity)) != TURBO_OK) {
    vec_destroy(&peers);
    result->status = TURBO_ENOMEM;
    return TURBO_ENOMEM;
  }
  for (size_t i = 0; i < capacity; ++i) {
    turbo_flow_discovery_peer_t *peer =
        (turbo_flow_discovery_peer_t *)vec_at(&peers, i);
    if (peer) peer->size = sizeof(*peer);
  }
  rc = source->fetch(source->ctx, &version,
                     (turbo_flow_discovery_peer_t *)vec_data(&peers), capacity, &count);
  if (rc != TURBO_OK) {
    result->status = rc;
    vec_destroy(&peers);
    return rc;
  }
  if (count > capacity) {
    result->status = TURBO_ENOSPC;
    vec_destroy(&peers);
    return TURBO_ENOSPC;
  }
  peer_list.registry_version = version;
  peer_list.peers = (const turbo_flow_discovery_peer_t *)vec_data_const(&peers);
  peer_list.peer_count = count;
  rc = turbo_flow_discovery_replace_peer_list(controller, &peer_list, result);
  vec_destroy(&peers);
  return rc;
}

uint64_t
turbo_flow_discovery_registry_version(const turbo_flow_discovery_controller_t *controller) {
  return controller ? controller->registry_version : 0u;
}

size_t turbo_flow_discovery_active_peer_count(const turbo_flow_discovery_controller_t *controller) {
  size_t count = 0u;
  if (!controller) return 0u;
  for (size_t i = 0; i < vec_size(&controller->slots); ++i) {
    const flow_discovery_slot_t *slot =
        (const flow_discovery_slot_t *)vec_at_const(&controller->slots, i);
    if (slot && slot->active) count += 1u;
  }
  return count;
}
