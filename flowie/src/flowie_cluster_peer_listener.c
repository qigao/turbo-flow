#include "flowie_cluster_peer_listener_internal.h"

#include "CoroNet.h"
#include "flow_coronet_execution.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

#define FLOWIE_CLUSTER_PEER_LISTENER_HOST_MAX 255u
#define FLOWIE_CLUSTER_PEER_LISTENER_PATH_MAX 4096u

typedef enum flowie_cluster_peer_listener_state_e {
  FLOWIE_CLUSTER_PEER_LISTENER_CREATED = 0,
  FLOWIE_CLUSTER_PEER_LISTENER_STARTING,
  FLOWIE_CLUSTER_PEER_LISTENER_RUNNING,
  FLOWIE_CLUSTER_PEER_LISTENER_CLOSING,
  FLOWIE_CLUSTER_PEER_LISTENER_DRAINED
} flowie_cluster_peer_listener_state_t;

typedef struct flowie_cluster_peer_listener_slot_s {
  struct flowie_cluster_peer_listener_s *listener;
  flowie_cluster_peer_link_t *link;
  tstr_t remote_node_id;
  uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  int in_use;
  int registered;
} flowie_cluster_peer_listener_slot_t;

struct flowie_cluster_peer_listener_s {
  tf_coronet_execution_t *execution;
  flowie_cluster_node_router_t *router;
  flowie_cluster_peer_listener_slot_t *slots;
  size_t max_connections;
  size_t max_payload_size;
  size_t queue_entries;
  size_t queue_bytes;
  uint32_t socket_timeout_ms;
  uint16_t bind_port;
  tstr_t bind_host;
  tstr_t cluster_id;
  tstr_t local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_t ca_file;
  tstr_t cert_file;
  tstr_t key_file;
  tstr_t key_password;
  flowie_cluster_peer_authorize_fn authorize;
  void *authorize_ctx;
  coro_socket_t *server;
  size_t active_handlers;
  size_t registered_links;
  uint64_t accepted_connections;
  uint64_t rejected_connections;
  uint64_t activated_links;
  uint64_t failed_links;
  int last_error;
  flowie_cluster_peer_listener_state_t state;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
};

static int flowie_cluster_peer_listener_nonzero(const uint8_t *value, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (value[index] != 0u) return 1;
  return 0;
}

static int flowie_cluster_peer_listener_text(tstr_v value, size_t maximum, int required) {
  if ((value.len != 0u && !value.data) || (required && value.len == 0u) || value.len > maximum)
    return TURBO_EINVAL;
  return value.len != 0u && memchr(value.data, '\0', value.len) ? TURBO_EPROTO : TURBO_OK;
}

static int flowie_cluster_peer_listener_text_compare(tstr_v left, tstr_v right) {
  size_t common = left.len < right.len ? left.len : right.len;
  int compared = memcmp(left.data, right.data, common);
  if (compared != 0) return compared;
  return left.len < right.len ? -1 : left.len > right.len ? 1 : 0;
}

static int
flowie_cluster_peer_listener_config_validate(const flowie_cluster_peer_listener_config_t *config) {
  const size_t identity_overhead =
      FLOWIE_CLUSTER_ID_MAX + FLOWIE_CLUSTER_LISTENER_ID_MAX + FLOWIE_CLUSTER_NODE_ID_MAX * 2u;
  size_t max_frame_size;
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PEER_LISTENER_ABI_V1 || !config->execution ||
      !config->execution->context || !config->router || config->max_connections == 0u ||
      config->max_connections > SIZE_MAX / sizeof(flowie_cluster_peer_listener_slot_t) ||
      config->max_payload_size == 0u || config->max_payload_size > UINT32_MAX ||
      config->queue_entries == 0u || config->queue_bytes == 0u || config->socket_timeout_ms == 0u ||
      config->bind_port == 0u || !config->authorize ||
      !flowie_cluster_peer_listener_nonzero(config->local_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE))
    return TURBO_EINVAL;
  if (config->max_payload_size > SIZE_MAX - identity_overhead - FLOWIE_CLUSTER_PEER_HEADER_SIZE)
    return TURBO_ERANGE;
  max_frame_size = FLOWIE_CLUSTER_PEER_HEADER_SIZE + identity_overhead + config->max_payload_size;
  if (max_frame_size > UINT32_MAX || config->queue_bytes < max_frame_size) return TURBO_EINVAL;
  rc = flowie_cluster_peer_listener_text(config->bind_host, FLOWIE_CLUSTER_PEER_LISTENER_HOST_MAX,
                                         1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_listener_text(config->cluster_id, FLOWIE_CLUSTER_ID_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_listener_text(config->local_node_id, FLOWIE_CLUSTER_NODE_ID_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_listener_text(config->ca_file, FLOWIE_CLUSTER_PEER_LISTENER_PATH_MAX,
                                           1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_listener_text(config->cert_file, FLOWIE_CLUSTER_PEER_LISTENER_PATH_MAX,
                                           1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_listener_text(config->key_file, FLOWIE_CLUSTER_PEER_LISTENER_PATH_MAX,
                                           1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_listener_text(config->key_password,
                                           FLOWIE_CLUSTER_PEER_LISTENER_PATH_MAX, 0);
  return rc;
}

static int
flowie_cluster_peer_listener_copy_config(flowie_cluster_peer_listener_t *listener,
                                         const flowie_cluster_peer_listener_config_t *config) {
  listener->bind_host = tstr_from_v(config->bind_host);
  listener->cluster_id = tstr_from_v(config->cluster_id);
  listener->local_node_id = tstr_from_v(config->local_node_id);
  listener->ca_file = tstr_from_v(config->ca_file);
  listener->cert_file = tstr_from_v(config->cert_file);
  listener->key_file = tstr_from_v(config->key_file);
  if (config->key_password.len != 0u) listener->key_password = tstr_from_v(config->key_password);
  return !listener->bind_host || !listener->cluster_id || !listener->local_node_id ||
                 !listener->ca_file || !listener->cert_file || !listener->key_file ||
                 (config->key_password.len != 0u && !listener->key_password)
             ? TURBO_ENOMEM
             : TURBO_OK;
}

int flowie_cluster_peer_listener_create(const flowie_cluster_peer_listener_config_t *config,
                                        flowie_cluster_peer_listener_t **out) {
  flowie_cluster_peer_listener_t *listener;
  size_t index;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_peer_listener_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  listener = (flowie_cluster_peer_listener_t *)calloc(1u, sizeof(*listener));
  if (!listener) return TURBO_ENOMEM;
  listener->slots = (flowie_cluster_peer_listener_slot_t *)calloc(config->max_connections,
                                                                  sizeof(*listener->slots));
  if (!listener->slots) {
    free(listener);
    return TURBO_ENOMEM;
  }
  rc = flowie_cluster_peer_listener_copy_config(listener, config);
  if (rc != TURBO_OK) goto fail;
  listener->execution = config->execution;
  listener->router = config->router;
  listener->max_connections = config->max_connections;
  listener->max_payload_size = config->max_payload_size;
  listener->queue_entries = config->queue_entries;
  listener->queue_bytes = config->queue_bytes;
  listener->socket_timeout_ms = config->socket_timeout_ms;
  listener->bind_port = config->bind_port;
  listener->authorize = config->authorize;
  listener->authorize_ctx = config->authorize_ctx;
  memcpy(listener->local_boot_id, config->local_boot_id, sizeof(listener->local_boot_id));
  for (index = 0u; index < listener->max_connections; ++index)
    listener->slots[index].listener = listener;
  turbo_mutex_init(&listener->mutex);
  turbo_cond_init(&listener->changed);
  *out = listener;
  return TURBO_OK;

fail:
  tstr_free(listener->bind_host);
  tstr_free(listener->cluster_id);
  tstr_free(listener->local_node_id);
  tstr_free(listener->ca_file);
  tstr_free(listener->cert_file);
  tstr_free(listener->key_file);
  tstr_free(listener->key_password);
  free(listener->slots);
  free(listener);
  return rc;
}

static flowie_cluster_peer_listener_slot_t *
flowie_cluster_peer_listener_claim(flowie_cluster_peer_listener_t *listener) {
  flowie_cluster_peer_listener_slot_t *slot = NULL;
  size_t index;
  turbo_mutex_lock(&listener->mutex);
  if (listener->state == FLOWIE_CLUSTER_PEER_LISTENER_RUNNING &&
      listener->active_handlers < listener->max_connections) {
    for (index = 0u; index < listener->max_connections; ++index) {
      if (!listener->slots[index].in_use) {
        slot = &listener->slots[index];
        slot->in_use = 1;
        ++listener->active_handlers;
        if (listener->accepted_connections != UINT64_MAX) ++listener->accepted_connections;
        break;
      }
    }
  }
  if (!slot && listener->rejected_connections != UINT64_MAX) ++listener->rejected_connections;
  turbo_mutex_unlock(&listener->mutex);
  return slot;
}

static int flowie_cluster_peer_listener_authorize(void *ctx, tstr_v peer_node_id,
                                                  const uint8_t *peer_boot_id,
                                                  const char *certificate_sha256) {
  flowie_cluster_peer_listener_slot_t *slot = (flowie_cluster_peer_listener_slot_t *)ctx;
  return slot->listener->authorize(slot->listener->authorize_ctx, peer_node_id, peer_boot_id,
                                   certificate_sha256);
}

static int flowie_cluster_peer_listener_active(void *ctx, flowie_cluster_peer_link_t *link,
                                               tstr_v peer_node_id, const uint8_t *peer_boot_id) {
  flowie_cluster_peer_listener_slot_t *slot = (flowie_cluster_peer_listener_slot_t *)ctx;
  flowie_cluster_peer_listener_t *listener = slot->listener;
  tstr_t owned_node_id;
  int closing;
  int rc;
  turbo_mutex_lock(&listener->mutex);
  closing = listener->state != FLOWIE_CLUSTER_PEER_LISTENER_RUNNING;
  turbo_mutex_unlock(&listener->mutex);
  if (closing) return TURBO_ESHUTDOWN;
  if (flowie_cluster_peer_listener_text_compare(peer_node_id, tstr_to_v(listener->local_node_id)) >=
      0)
    return TURBO_EPERM;
  owned_node_id = tstr_from_v(peer_node_id);
  if (!owned_node_id) return TURBO_ENOMEM;
  rc = flowie_cluster_node_router_register_link(listener->router, peer_node_id, peer_boot_id, link);
  if (rc != TURBO_OK) {
    tstr_free(owned_node_id);
    return rc;
  }
  turbo_mutex_lock(&listener->mutex);
  slot->remote_node_id = owned_node_id;
  memcpy(slot->remote_boot_id, peer_boot_id, sizeof(slot->remote_boot_id));
  slot->registered = 1;
  ++listener->registered_links;
  if (listener->activated_links != UINT64_MAX) ++listener->activated_links;
  turbo_cond_broadcast(&listener->changed);
  turbo_mutex_unlock(&listener->mutex);
  return TURBO_OK;
}

static int flowie_cluster_peer_listener_receive(void *ctx,
                                                const flowie_cluster_peer_frame_t *frame) {
  flowie_cluster_peer_listener_slot_t *slot = (flowie_cluster_peer_listener_slot_t *)ctx;
  return flowie_cluster_node_router_receive(slot->listener->router, frame);
}

static void flowie_cluster_peer_listener_unregister(flowie_cluster_peer_listener_slot_t *slot) {
  flowie_cluster_peer_listener_t *listener = slot->listener;
  int rc;
  if (!slot->registered) return;
  do {
    rc = flowie_cluster_node_router_unregister_link(
        listener->router, tstr_to_v(slot->remote_node_id), slot->remote_boot_id, slot->link);
    if (rc == TURBO_EBUSY) coro_sleep(listener->execution->context, 1u);
  } while (rc == TURBO_EBUSY);
  turbo_mutex_lock(&listener->mutex);
  if (rc != TURBO_OK && rc != TURBO_ENOENT) listener->last_error = rc;
  slot->registered = 0;
  if (listener->registered_links != 0u) --listener->registered_links;
  turbo_cond_broadcast(&listener->changed);
  turbo_mutex_unlock(&listener->mutex);
}

static void flowie_cluster_peer_listener_release(flowie_cluster_peer_listener_slot_t *slot,
                                                 int status) {
  flowie_cluster_peer_listener_t *listener = slot->listener;
  tstr_free(slot->remote_node_id);
  turbo_mutex_lock(&listener->mutex);
  slot->remote_node_id = NULL;
  memset(slot->remote_boot_id, 0, sizeof(slot->remote_boot_id));
  slot->link = NULL;
  slot->in_use = 0;
  if (listener->active_handlers != 0u) --listener->active_handlers;
  if (status != TURBO_OK && status != TURBO_EOF && status != TURBO_ECANCELED &&
      status != TURBO_ESHUTDOWN) {
    listener->last_error = status;
    if (listener->failed_links != UINT64_MAX) ++listener->failed_links;
  }
  turbo_cond_broadcast(&listener->changed);
  turbo_mutex_unlock(&listener->mutex);
}

static void flowie_cluster_peer_listener_handle(coro_socket_t *socket, void *ctx) {
  flowie_cluster_peer_listener_t *listener = (flowie_cluster_peer_listener_t *)ctx;
  flowie_cluster_peer_listener_slot_t *slot = flowie_cluster_peer_listener_claim(listener);
  flowie_cluster_peer_link_config_t config = FLOWIE_CLUSTER_PEER_LINK_CONFIG_INIT;
  int rc;
  if (!slot) return;
  config.role = FLOWIE_CLUSTER_PEER_ROLE_RESPONDER;
  config.max_payload_size = listener->max_payload_size;
  config.queue_entries = listener->queue_entries;
  config.queue_bytes = listener->queue_bytes;
  config.cluster_id = tstr_to_v(listener->cluster_id);
  config.local_node_id = tstr_to_v(listener->local_node_id);
  memcpy(config.local_boot_id, listener->local_boot_id, sizeof(config.local_boot_id));
  config.authorize = flowie_cluster_peer_listener_authorize;
  config.active = flowie_cluster_peer_listener_active;
  config.receive = flowie_cluster_peer_listener_receive;
  config.user_data = slot;
  rc = flowie_cluster_peer_link_create(&config, &slot->link);
  if (rc == TURBO_OK) rc = flowie_cluster_peer_link_run(slot->link, socket);
  flowie_cluster_peer_listener_unregister(slot);
  if (slot->link) {
    int destroy_rc = flowie_cluster_peer_link_destroy(slot->link);
    if (rc == TURBO_OK) rc = destroy_rc;
  }
  flowie_cluster_peer_listener_release(slot, rc);
}

static void flowie_cluster_peer_listener_socket_closed(void *ctx) {
  flowie_cluster_peer_listener_t *listener = (flowie_cluster_peer_listener_t *)ctx;
  turbo_mutex_lock(&listener->mutex);
  turbo_cond_broadcast(&listener->changed);
  turbo_mutex_unlock(&listener->mutex);
}

static int flowie_cluster_peer_listener_start_call(void *ctx) {
  flowie_cluster_peer_listener_t *listener = (flowie_cluster_peer_listener_t *)ctx;
  int rc;
  listener->server = coro_socket_create(listener->execution->context, CORO_SOCKET_TLS);
  rc = listener->server ? TURBO_OK : TURBO_ENOMEM;
  if (rc == TURBO_OK)
    rc = coro_socket_set_server_admission_limit(listener->server, listener->max_connections);
  if (rc == TURBO_OK) coro_socket_set_timeout(listener->server, listener->socket_timeout_ms);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_tls_server_configure(
        listener->server, listener->ca_file, listener->cert_file, listener->key_file,
        listener->key_password ? listener->key_password : NULL);
  if (rc == TURBO_OK)
    rc = coro_socket_listen_on_ex(listener->server, listener->bind_host, listener->bind_port,
                                  flowie_cluster_peer_listener_handle, listener,
                                  flowie_cluster_peer_listener_socket_closed, listener);
  turbo_mutex_lock(&listener->mutex);
  if (rc == TURBO_OK) {
    listener->state = FLOWIE_CLUSTER_PEER_LISTENER_RUNNING;
  } else {
    listener->state = FLOWIE_CLUSTER_PEER_LISTENER_CREATED;
    listener->last_error = rc;
  }
  turbo_cond_broadcast(&listener->changed);
  turbo_mutex_unlock(&listener->mutex);
  if (rc != TURBO_OK) {
    if (listener->server) coro_socket_destroy(listener->server);
    listener->server = NULL;
  }
  return rc;
}

int flowie_cluster_peer_listener_start(flowie_cluster_peer_listener_t *listener,
                                       uint64_t timeout_ns) {
  int rc;
  if (!listener || timeout_ns == 0u) return TURBO_EINVAL;
  turbo_mutex_lock(&listener->mutex);
  if (listener->state == FLOWIE_CLUSTER_PEER_LISTENER_CREATED) {
    listener->state = FLOWIE_CLUSTER_PEER_LISTENER_STARTING;
    rc = TURBO_OK;
  } else {
    rc = TURBO_EALREADY;
  }
  turbo_mutex_unlock(&listener->mutex);
  if (rc != TURBO_OK) return rc;
  rc = tf_coronet_execution_call(listener->execution, flowie_cluster_peer_listener_start_call,
                                 listener, timeout_ns);
  if (rc != TURBO_OK) {
    turbo_mutex_lock(&listener->mutex);
    if (listener->state == FLOWIE_CLUSTER_PEER_LISTENER_STARTING)
      listener->state = FLOWIE_CLUSTER_PEER_LISTENER_CREATED;
    turbo_mutex_unlock(&listener->mutex);
  }
  return rc;
}

static int flowie_cluster_peer_listener_close_call(void *ctx) {
  flowie_cluster_peer_listener_t *listener = (flowie_cluster_peer_listener_t *)ctx;
  size_t index;
  for (index = 0u; index < listener->max_connections; ++index) {
    flowie_cluster_peer_link_t *link;
    turbo_mutex_lock(&listener->mutex);
    link = listener->slots[index].link;
    turbo_mutex_unlock(&listener->mutex);
    if (link) (void)flowie_cluster_peer_link_close(link);
  }
  return listener->server ? coro_socket_server_stop(listener->server) : TURBO_OK;
}

int flowie_cluster_peer_listener_close(flowie_cluster_peer_listener_t *listener,
                                       uint64_t timeout_ns) {
  flowie_cluster_peer_listener_state_t previous_state;
  int rc;
  if (!listener || timeout_ns == 0u) return TURBO_EINVAL;
  turbo_mutex_lock(&listener->mutex);
  previous_state = listener->state;
  if (listener->state == FLOWIE_CLUSTER_PEER_LISTENER_STARTING) {
    rc = TURBO_EBUSY;
  } else if (listener->state == FLOWIE_CLUSTER_PEER_LISTENER_CLOSING ||
             listener->state == FLOWIE_CLUSTER_PEER_LISTENER_DRAINED) {
    rc = TURBO_EALREADY;
  } else {
    listener->state = FLOWIE_CLUSTER_PEER_LISTENER_CLOSING;
    turbo_cond_broadcast(&listener->changed);
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&listener->mutex);
  if (rc != TURBO_OK) return rc;
  rc = tf_coronet_execution_call(listener->execution, flowie_cluster_peer_listener_close_call,
                                 listener, timeout_ns);
  if (rc != TURBO_OK) {
    turbo_mutex_lock(&listener->mutex);
    if (listener->state == FLOWIE_CLUSTER_PEER_LISTENER_CLOSING) listener->state = previous_state;
    listener->last_error = rc;
    turbo_mutex_unlock(&listener->mutex);
  }
  return rc;
}

static int flowie_cluster_peer_listener_stopped_call(void *ctx) {
  flowie_cluster_peer_listener_t *listener = (flowie_cluster_peer_listener_t *)ctx;
  return !listener->server || coro_socket_server_is_stopped(listener->server) ? TURBO_OK
                                                                              : TURBO_EBUSY;
}

static int flowie_cluster_peer_listener_destroy_server_call(void *ctx) {
  flowie_cluster_peer_listener_t *listener = (flowie_cluster_peer_listener_t *)ctx;
  if (!listener->server) return TURBO_OK;
  if (!coro_socket_server_is_stopped(listener->server)) return TURBO_EBUSY;
  coro_socket_destroy(listener->server);
  listener->server = NULL;
  return TURBO_OK;
}

int flowie_cluster_peer_listener_drain(flowie_cluster_peer_listener_t *listener,
                                       uint64_t timeout_ns) {
  uint64_t start_ns;
  uint64_t deadline_ns;
  int rc;
  if (!listener) return TURBO_EINVAL;
  if (coro_context_current() == listener->execution->context) return TURBO_EBUSY;
  start_ns = turbo_hrtime();
  deadline_ns = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + timeout_ns;
  for (;;) {
    uint64_t now_ns;
    uint64_t remaining_ns;
    int handlers_done;
    turbo_mutex_lock(&listener->mutex);
    if (listener->state != FLOWIE_CLUSTER_PEER_LISTENER_CLOSING) {
      turbo_mutex_unlock(&listener->mutex);
      return TURBO_EBUSY;
    }
    handlers_done = listener->active_handlers == 0u && listener->registered_links == 0u;
    turbo_mutex_unlock(&listener->mutex);
    now_ns = turbo_hrtime();
    if (deadline_ns != UINT64_MAX && now_ns >= deadline_ns)
      return timeout_ns == 0u ? TURBO_EBUSY : TURBO_ETIMEDOUT;
    remaining_ns = deadline_ns == UINT64_MAX ? UINT64_MAX : deadline_ns - now_ns;
    rc = tf_coronet_execution_call(listener->execution, flowie_cluster_peer_listener_stopped_call,
                                   listener, remaining_ns);
    if (rc != TURBO_OK && rc != TURBO_EBUSY) return rc;
    if (handlers_done && rc == TURBO_OK) break;
    turbo_sleep_ms(1u);
  }
  if (deadline_ns == UINT64_MAX) {
    rc = tf_coronet_execution_call(listener->execution,
                                   flowie_cluster_peer_listener_destroy_server_call, listener,
                                   UINT64_MAX);
  } else {
    uint64_t now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) return timeout_ns == 0u ? TURBO_EBUSY : TURBO_ETIMEDOUT;
    rc = tf_coronet_execution_call(listener->execution,
                                   flowie_cluster_peer_listener_destroy_server_call, listener,
                                   deadline_ns - now_ns);
  }
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&listener->mutex);
  listener->state = FLOWIE_CLUSTER_PEER_LISTENER_DRAINED;
  turbo_cond_broadcast(&listener->changed);
  turbo_mutex_unlock(&listener->mutex);
  return TURBO_OK;
}

int flowie_cluster_peer_listener_snapshot(flowie_cluster_peer_listener_t *listener,
                                          flowie_cluster_peer_listener_snapshot_t *out) {
  if (!listener || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PEER_LISTENER_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&listener->mutex);
  out->active_handlers = listener->active_handlers;
  out->registered_links = listener->registered_links;
  out->accepted_connections = listener->accepted_connections;
  out->rejected_connections = listener->rejected_connections;
  out->activated_links = listener->activated_links;
  out->failed_links = listener->failed_links;
  out->running = listener->state == FLOWIE_CLUSTER_PEER_LISTENER_RUNNING;
  out->closing = listener->state == FLOWIE_CLUSTER_PEER_LISTENER_CLOSING ||
                 listener->state == FLOWIE_CLUSTER_PEER_LISTENER_DRAINED;
  out->last_error = listener->last_error;
  turbo_mutex_unlock(&listener->mutex);
  return TURBO_OK;
}

int flowie_cluster_peer_listener_destroy(flowie_cluster_peer_listener_t *listener) {
  size_t index;
  if (!listener) return TURBO_EINVAL;
  turbo_mutex_lock(&listener->mutex);
  if (listener->state != FLOWIE_CLUSTER_PEER_LISTENER_DRAINED || listener->server ||
      listener->active_handlers != 0u || listener->registered_links != 0u) {
    turbo_mutex_unlock(&listener->mutex);
    return TURBO_EBUSY;
  }
  turbo_mutex_unlock(&listener->mutex);
  for (index = 0u; index < listener->max_connections; ++index)
    if (listener->slots[index].in_use || listener->slots[index].link ||
        listener->slots[index].registered)
      return TURBO_EBUSY;
  turbo_cond_destroy(&listener->changed);
  turbo_mutex_destroy(&listener->mutex);
  tstr_free(listener->bind_host);
  tstr_free(listener->cluster_id);
  tstr_free(listener->local_node_id);
  tstr_free(listener->ca_file);
  tstr_free(listener->cert_file);
  tstr_free(listener->key_file);
  tstr_free(listener->key_password);
  free(listener->slots);
  free(listener);
  return TURBO_OK;
}
