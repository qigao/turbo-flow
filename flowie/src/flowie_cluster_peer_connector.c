#include "flowie_cluster_peer_connector_internal.h"

#include "CoroNet.h"
#include "flow_coronet_execution.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

#define FLOWIE_CLUSTER_PEER_CONNECTOR_HOST_MAX 255u
#define FLOWIE_CLUSTER_PEER_CONNECTOR_PATH_MAX 4096u
#define FLOWIE_CLUSTER_PEER_CONNECTOR_RETRY_POLL_MS 10u

typedef enum flowie_cluster_peer_connector_state_e {
  FLOWIE_CLUSTER_PEER_CONNECTOR_CREATED = 0,
  FLOWIE_CLUSTER_PEER_CONNECTOR_STARTING,
  FLOWIE_CLUSTER_PEER_CONNECTOR_RUNNING,
  FLOWIE_CLUSTER_PEER_CONNECTOR_CLOSING,
  FLOWIE_CLUSTER_PEER_CONNECTOR_DRAINED
} flowie_cluster_peer_connector_state_t;

struct flowie_cluster_peer_connector_s {
  tf_coronet_execution_t *execution;
  flowie_cluster_node_router_t *router;
  size_t max_payload_size;
  size_t queue_entries;
  size_t queue_bytes;
  uint32_t socket_timeout_ms;
  uint32_t retry_delay_ms;
  uint16_t remote_port;
  tstr_t remote_host;
  tstr_t cluster_id;
  tstr_t local_node_id;
  uint8_t local_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_t remote_node_id;
  uint8_t remote_boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  tstr_t ca_file;
  tstr_t cert_file;
  tstr_t key_file;
  tstr_t key_password;
  flowie_cluster_peer_authorize_fn authorize;
  void *authorize_ctx;
  flowie_cluster_peer_link_t *link;
  coro_socket_t *socket;
  uint64_t connect_attempts;
  uint64_t activated_links;
  int task_running;
  int registered;
  int terminal_error;
  int last_error;
  flowie_cluster_peer_connector_state_t state;
  turbo_mutex_t mutex;
  turbo_cond_t changed;
};

static int flowie_cluster_peer_connector_nonzero(const uint8_t *value, size_t size) {
  size_t index;
  for (index = 0u; index < size; ++index)
    if (value[index] != 0u) return 1;
  return 0;
}

static int flowie_cluster_peer_connector_text(tstr_v value, size_t maximum, int required) {
  if ((value.len != 0u && !value.data) || (required && value.len == 0u) || value.len > maximum)
    return TURBO_EINVAL;
  return value.len != 0u && memchr(value.data, '\0', value.len) ? TURBO_EPROTO : TURBO_OK;
}

static int flowie_cluster_peer_connector_text_compare(tstr_v left, tstr_v right) {
  size_t common = left.len < right.len ? left.len : right.len;
  int compared = memcmp(left.data, right.data, common);
  if (compared != 0) return compared;
  return left.len < right.len ? -1 : left.len > right.len ? 1 : 0;
}

static int flowie_cluster_peer_connector_config_validate(
    const flowie_cluster_peer_connector_config_t *config) {
  const size_t identity_overhead =
      FLOWIE_CLUSTER_ID_MAX + FLOWIE_CLUSTER_LISTENER_ID_MAX + FLOWIE_CLUSTER_NODE_ID_MAX * 2u;
  size_t max_frame_size;
  int rc;
  if (!config || config->size != sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PEER_CONNECTOR_ABI_V1 || !config->execution ||
      !config->execution->context || !config->router || config->max_payload_size == 0u ||
      config->max_payload_size > UINT32_MAX || config->queue_entries == 0u ||
      config->queue_bytes == 0u || config->socket_timeout_ms == 0u ||
      config->retry_delay_ms == 0u || config->remote_port == 0u || !config->authorize ||
      !flowie_cluster_peer_connector_nonzero(config->local_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE) ||
      !flowie_cluster_peer_connector_nonzero(config->remote_boot_id, FLOWIE_CLUSTER_BOOT_ID_SIZE))
    return TURBO_EINVAL;
  if (config->max_payload_size > SIZE_MAX - identity_overhead - FLOWIE_CLUSTER_PEER_HEADER_SIZE)
    return TURBO_ERANGE;
  max_frame_size = FLOWIE_CLUSTER_PEER_HEADER_SIZE + identity_overhead + config->max_payload_size;
  if (max_frame_size > UINT32_MAX || config->queue_bytes < max_frame_size) return TURBO_EINVAL;
  rc = flowie_cluster_peer_connector_text(config->remote_host,
                                          FLOWIE_CLUSTER_PEER_CONNECTOR_HOST_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connector_text(config->cluster_id, FLOWIE_CLUSTER_ID_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connector_text(config->local_node_id, FLOWIE_CLUSTER_NODE_ID_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connector_text(config->remote_node_id, FLOWIE_CLUSTER_NODE_ID_MAX, 1);
  if (rc == TURBO_OK && flowie_cluster_peer_connector_text_compare(config->local_node_id,
                                                                   config->remote_node_id) >= 0)
    rc = TURBO_EPERM;
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connector_text(config->ca_file, FLOWIE_CLUSTER_PEER_CONNECTOR_PATH_MAX,
                                            1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connector_text(config->cert_file,
                                            FLOWIE_CLUSTER_PEER_CONNECTOR_PATH_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connector_text(config->key_file,
                                            FLOWIE_CLUSTER_PEER_CONNECTOR_PATH_MAX, 1);
  if (rc == TURBO_OK)
    rc = flowie_cluster_peer_connector_text(config->key_password,
                                            FLOWIE_CLUSTER_PEER_CONNECTOR_PATH_MAX, 0);
  return rc;
}

static int
flowie_cluster_peer_connector_copy_config(flowie_cluster_peer_connector_t *connector,
                                          const flowie_cluster_peer_connector_config_t *config) {
  connector->remote_host = tstr_from_v(config->remote_host);
  connector->cluster_id = tstr_from_v(config->cluster_id);
  connector->local_node_id = tstr_from_v(config->local_node_id);
  connector->remote_node_id = tstr_from_v(config->remote_node_id);
  connector->ca_file = tstr_from_v(config->ca_file);
  connector->cert_file = tstr_from_v(config->cert_file);
  connector->key_file = tstr_from_v(config->key_file);
  if (config->key_password.len != 0u) connector->key_password = tstr_from_v(config->key_password);
  return !connector->remote_host || !connector->cluster_id || !connector->local_node_id ||
                 !connector->remote_node_id || !connector->ca_file || !connector->cert_file ||
                 !connector->key_file ||
                 (config->key_password.len != 0u && !connector->key_password)
             ? TURBO_ENOMEM
             : TURBO_OK;
}

int flowie_cluster_peer_connector_create(const flowie_cluster_peer_connector_config_t *config,
                                         flowie_cluster_peer_connector_t **out) {
  flowie_cluster_peer_connector_t *connector;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_peer_connector_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  connector = (flowie_cluster_peer_connector_t *)calloc(1u, sizeof(*connector));
  if (!connector) return TURBO_ENOMEM;
  rc = flowie_cluster_peer_connector_copy_config(connector, config);
  if (rc != TURBO_OK) goto fail;
  connector->execution = config->execution;
  connector->router = config->router;
  connector->max_payload_size = config->max_payload_size;
  connector->queue_entries = config->queue_entries;
  connector->queue_bytes = config->queue_bytes;
  connector->socket_timeout_ms = config->socket_timeout_ms;
  connector->retry_delay_ms = config->retry_delay_ms;
  connector->remote_port = config->remote_port;
  connector->authorize = config->authorize;
  connector->authorize_ctx = config->authorize_ctx;
  memcpy(connector->local_boot_id, config->local_boot_id, sizeof(connector->local_boot_id));
  memcpy(connector->remote_boot_id, config->remote_boot_id, sizeof(connector->remote_boot_id));
  turbo_mutex_init(&connector->mutex);
  turbo_cond_init(&connector->changed);
  *out = connector;
  return TURBO_OK;

fail:
  tstr_free(connector->remote_host);
  tstr_free(connector->cluster_id);
  tstr_free(connector->local_node_id);
  tstr_free(connector->remote_node_id);
  tstr_free(connector->ca_file);
  tstr_free(connector->cert_file);
  tstr_free(connector->key_file);
  tstr_free(connector->key_password);
  free(connector);
  return rc;
}

static int flowie_cluster_peer_connector_authorize(void *ctx, tstr_v peer_node_id,
                                                   const uint8_t *peer_boot_id,
                                                   const char *certificate_sha256) {
  flowie_cluster_peer_connector_t *connector = (flowie_cluster_peer_connector_t *)ctx;
  return connector->authorize(connector->authorize_ctx, peer_node_id, peer_boot_id,
                              certificate_sha256);
}

static int flowie_cluster_peer_connector_active(void *ctx, flowie_cluster_peer_link_t *link,
                                                tstr_v peer_node_id, const uint8_t *peer_boot_id) {
  flowie_cluster_peer_connector_t *connector = (flowie_cluster_peer_connector_t *)ctx;
  int closing;
  int rc;
  turbo_mutex_lock(&connector->mutex);
  closing = connector->state != FLOWIE_CLUSTER_PEER_CONNECTOR_RUNNING;
  turbo_mutex_unlock(&connector->mutex);
  if (closing) return TURBO_ESHUTDOWN;
  rc =
      flowie_cluster_node_router_register_link(connector->router, peer_node_id, peer_boot_id, link);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&connector->mutex);
  connector->registered = 1;
  if (connector->activated_links != UINT64_MAX) ++connector->activated_links;
  connector->last_error = TURBO_OK;
  connector->terminal_error = 0;
  turbo_cond_broadcast(&connector->changed);
  turbo_mutex_unlock(&connector->mutex);
  return TURBO_OK;
}

static int flowie_cluster_peer_connector_receive(void *ctx,
                                                 const flowie_cluster_peer_frame_t *frame) {
  flowie_cluster_peer_connector_t *connector = (flowie_cluster_peer_connector_t *)ctx;
  return flowie_cluster_node_router_receive(connector->router, frame);
}

static int flowie_cluster_peer_connector_is_closing(flowie_cluster_peer_connector_t *connector) {
  int closing;
  turbo_mutex_lock(&connector->mutex);
  closing = connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_CLOSING ||
            connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_DRAINED;
  turbo_mutex_unlock(&connector->mutex);
  return closing;
}

static void flowie_cluster_peer_connector_unregister(flowie_cluster_peer_connector_t *connector) {
  int registered;
  int rc;
  turbo_mutex_lock(&connector->mutex);
  registered = connector->registered;
  turbo_mutex_unlock(&connector->mutex);
  if (!registered) return;
  do {
    rc = flowie_cluster_node_router_unregister_link(connector->router,
                                                    tstr_to_v(connector->remote_node_id),
                                                    connector->remote_boot_id, connector->link);
    if (rc == TURBO_EBUSY) coro_sleep(connector->execution->context, 1u);
  } while (rc == TURBO_EBUSY);
  turbo_mutex_lock(&connector->mutex);
  connector->registered = 0;
  if (rc != TURBO_OK && rc != TURBO_ENOENT) connector->last_error = rc;
  turbo_cond_broadcast(&connector->changed);
  turbo_mutex_unlock(&connector->mutex);
}

static int flowie_cluster_peer_connector_retryable(int rc) {
  switch (rc) {
  case TURBO_EOF:
  case TURBO_ETIMEDOUT:
  case TURBO_ECONNABORTED:
  case TURBO_ECONNREFUSED:
  case TURBO_ECONNRESET:
  case TURBO_EHOSTUNREACH:
  case TURBO_ENETDOWN:
  case TURBO_ENETUNREACH:
  case TURBO_EIO:
    return 1;
  default:
    return 0;
  }
}

static void flowie_cluster_peer_connector_wait_retry(flowie_cluster_peer_connector_t *connector) {
  uint32_t waited_ms = 0u;
  while (waited_ms < connector->retry_delay_ms &&
         !flowie_cluster_peer_connector_is_closing(connector)) {
    uint32_t remaining_ms = connector->retry_delay_ms - waited_ms;
    uint32_t step_ms = remaining_ms < FLOWIE_CLUSTER_PEER_CONNECTOR_RETRY_POLL_MS
                           ? remaining_ms
                           : FLOWIE_CLUSTER_PEER_CONNECTOR_RETRY_POLL_MS;
    coro_sleep(connector->execution->context, step_ms);
    waited_ms += step_ms;
  }
}

static void
flowie_cluster_peer_connector_cleanup_attempt(flowie_cluster_peer_connector_t *connector,
                                              flowie_cluster_peer_link_t *link,
                                              coro_socket_t *socket) {
  flowie_cluster_peer_connector_unregister(connector);
  turbo_mutex_lock(&connector->mutex);
  connector->link = NULL;
  connector->socket = NULL;
  turbo_cond_broadcast(&connector->changed);
  turbo_mutex_unlock(&connector->mutex);
  if (socket) coro_socket_destroy(socket);
  if (link) (void)flowie_cluster_peer_link_destroy(link);
}

static void flowie_cluster_peer_connector_task(coro_t *coroutine, void *ctx) {
  flowie_cluster_peer_connector_t *connector = (flowie_cluster_peer_connector_t *)ctx;
  int rc = TURBO_OK;
  (void)coroutine;
  while (!flowie_cluster_peer_connector_is_closing(connector)) {
    flowie_cluster_peer_link_config_t config = FLOWIE_CLUSTER_PEER_LINK_CONFIG_INIT;
    flowie_cluster_peer_link_t *link = NULL;
    coro_socket_t *socket = NULL;
    config.role = FLOWIE_CLUSTER_PEER_ROLE_INITIATOR;
    config.max_payload_size = connector->max_payload_size;
    config.queue_entries = connector->queue_entries;
    config.queue_bytes = connector->queue_bytes;
    config.cluster_id = tstr_to_v(connector->cluster_id);
    config.local_node_id = tstr_to_v(connector->local_node_id);
    config.remote_node_id = tstr_to_v(connector->remote_node_id);
    memcpy(config.local_boot_id, connector->local_boot_id, sizeof(config.local_boot_id));
    memcpy(config.remote_boot_id, connector->remote_boot_id, sizeof(config.remote_boot_id));
    config.authorize = flowie_cluster_peer_connector_authorize;
    config.active = flowie_cluster_peer_connector_active;
    config.receive = flowie_cluster_peer_connector_receive;
    config.user_data = connector;
    turbo_mutex_lock(&connector->mutex);
    if (connector->connect_attempts != UINT64_MAX) ++connector->connect_attempts;
    turbo_mutex_unlock(&connector->mutex);
    rc = flowie_cluster_peer_link_create(&config, &link);
    if (rc == TURBO_OK) socket = coro_socket_create(connector->execution->context, CORO_SOCKET_TLS);
    if (rc == TURBO_OK && !socket) rc = TURBO_ENOMEM;
    turbo_mutex_lock(&connector->mutex);
    connector->link = link;
    connector->socket = socket;
    turbo_mutex_unlock(&connector->mutex);
    if (rc == TURBO_OK) coro_socket_set_timeout(socket, connector->socket_timeout_ms);
    if (rc == TURBO_OK)
      rc = flowie_cluster_peer_tls_client_configure(
          socket, connector->ca_file, connector->cert_file, connector->key_file,
          connector->key_password ? connector->key_password : NULL);
    if (rc == TURBO_OK)
      rc = coro_socket_connect(socket, connector->remote_host, connector->remote_port);
    if (rc == TURBO_OK) rc = flowie_cluster_peer_link_run(link, socket);
    flowie_cluster_peer_connector_cleanup_attempt(connector, link, socket);
    if (flowie_cluster_peer_connector_is_closing(connector)) break;
    turbo_mutex_lock(&connector->mutex);
    connector->last_error = rc;
    if (!flowie_cluster_peer_connector_retryable(rc)) connector->terminal_error = 1;
    turbo_mutex_unlock(&connector->mutex);
    if (!flowie_cluster_peer_connector_retryable(rc)) break;
    flowie_cluster_peer_connector_wait_retry(connector);
  }
  turbo_mutex_lock(&connector->mutex);
  connector->task_running = 0;
  turbo_cond_broadcast(&connector->changed);
  turbo_mutex_unlock(&connector->mutex);
}

static int flowie_cluster_peer_connector_start_call(void *ctx) {
  flowie_cluster_peer_connector_t *connector = (flowie_cluster_peer_connector_t *)ctx;
  int rc = coro_context_spawn(connector->execution->context, flowie_cluster_peer_connector_task,
                              connector);
  turbo_mutex_lock(&connector->mutex);
  if (rc == TURBO_OK) {
    connector->state = FLOWIE_CLUSTER_PEER_CONNECTOR_RUNNING;
    connector->task_running = 1;
  } else {
    connector->state = FLOWIE_CLUSTER_PEER_CONNECTOR_CREATED;
    connector->last_error = rc;
  }
  turbo_cond_broadcast(&connector->changed);
  turbo_mutex_unlock(&connector->mutex);
  return rc;
}

int flowie_cluster_peer_connector_start(flowie_cluster_peer_connector_t *connector,
                                        uint64_t timeout_ns) {
  int rc;
  if (!connector || timeout_ns == 0u) return TURBO_EINVAL;
  turbo_mutex_lock(&connector->mutex);
  if (connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_CREATED) {
    connector->state = FLOWIE_CLUSTER_PEER_CONNECTOR_STARTING;
    rc = TURBO_OK;
  } else {
    rc = TURBO_EALREADY;
  }
  turbo_mutex_unlock(&connector->mutex);
  if (rc != TURBO_OK) return rc;
  rc = tf_coronet_execution_call(connector->execution, flowie_cluster_peer_connector_start_call,
                                 connector, timeout_ns);
  if (rc != TURBO_OK) {
    turbo_mutex_lock(&connector->mutex);
    if (connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_STARTING)
      connector->state = FLOWIE_CLUSTER_PEER_CONNECTOR_CREATED;
    turbo_mutex_unlock(&connector->mutex);
  }
  return rc;
}

static int flowie_cluster_peer_connector_close_call(void *ctx) {
  flowie_cluster_peer_connector_t *connector = (flowie_cluster_peer_connector_t *)ctx;
  flowie_cluster_peer_link_t *link;
  flowie_cluster_peer_link_state_t link_state = FLOWIE_CLUSTER_PEER_LINK_CLOSED;
  coro_socket_t *socket;
  int rc = TURBO_OK;
  turbo_mutex_lock(&connector->mutex);
  link = connector->link;
  socket = connector->socket;
  turbo_mutex_unlock(&connector->mutex);
  if (link) {
    link_state = flowie_cluster_peer_link_state(link);
    rc = flowie_cluster_peer_link_close(link);
    if (rc == TURBO_EALREADY) rc = TURBO_OK;
  }
  if (rc == TURBO_OK && socket &&
      (link_state == FLOWIE_CLUSTER_PEER_LINK_CREATED ||
       link_state == FLOWIE_CLUSTER_PEER_LINK_CLOSED)) {
    rc = coro_socket_interrupt_wait(socket, TURBO_ECANCELED);
    if (rc == TURBO_EALREADY || rc == TURBO_EBUSY) rc = TURBO_OK;
  }
  return rc;
}

int flowie_cluster_peer_connector_close(flowie_cluster_peer_connector_t *connector,
                                        uint64_t timeout_ns) {
  flowie_cluster_peer_connector_state_t previous_state;
  int rc;
  if (!connector || timeout_ns == 0u) return TURBO_EINVAL;
  turbo_mutex_lock(&connector->mutex);
  previous_state = connector->state;
  if (connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_STARTING) rc = TURBO_EBUSY;
  else if (connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_CLOSING ||
           connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_DRAINED)
    rc = TURBO_EALREADY;
  else {
    connector->state = FLOWIE_CLUSTER_PEER_CONNECTOR_CLOSING;
    turbo_cond_broadcast(&connector->changed);
    rc = TURBO_OK;
  }
  turbo_mutex_unlock(&connector->mutex);
  if (rc != TURBO_OK) return rc;
  rc = tf_coronet_execution_call(connector->execution, flowie_cluster_peer_connector_close_call,
                                 connector, timeout_ns);
  if (rc != TURBO_OK) {
    turbo_mutex_lock(&connector->mutex);
    if (connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_CLOSING)
      connector->state = previous_state;
    connector->last_error = rc;
    turbo_mutex_unlock(&connector->mutex);
  }
  return rc;
}

int flowie_cluster_peer_connector_drain(flowie_cluster_peer_connector_t *connector,
                                        uint64_t timeout_ns) {
  uint64_t start_ns;
  uint64_t deadline_ns;
  int rc = TURBO_OK;
  if (!connector) return TURBO_EINVAL;
  if (coro_context_current() == connector->execution->context) return TURBO_EBUSY;
  start_ns = turbo_hrtime();
  deadline_ns = timeout_ns == UINT64_MAX || timeout_ns > UINT64_MAX - start_ns
                    ? UINT64_MAX
                    : start_ns + timeout_ns;
  turbo_mutex_lock(&connector->mutex);
  if (connector->state != FLOWIE_CLUSTER_PEER_CONNECTOR_CLOSING) rc = TURBO_EBUSY;
  while (rc == TURBO_OK && (connector->task_running || connector->registered)) {
    uint64_t now_ns;
    if (deadline_ns == UINT64_MAX) {
      turbo_cond_wait(&connector->changed, &connector->mutex);
      continue;
    }
    now_ns = turbo_hrtime();
    if (now_ns >= deadline_ns) {
      rc = timeout_ns == 0u ? TURBO_EBUSY : TURBO_ETIMEDOUT;
      break;
    }
    (void)turbo_cond_timedwait(&connector->changed, &connector->mutex, deadline_ns - now_ns);
  }
  if (rc == TURBO_OK) connector->state = FLOWIE_CLUSTER_PEER_CONNECTOR_DRAINED;
  turbo_mutex_unlock(&connector->mutex);
  return rc;
}

int flowie_cluster_peer_connector_snapshot(flowie_cluster_peer_connector_t *connector,
                                           flowie_cluster_peer_connector_snapshot_t *out) {
  if (!connector || !out || out->size != sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PEER_CONNECTOR_ABI_V1)
    return TURBO_EINVAL;
  turbo_mutex_lock(&connector->mutex);
  out->connect_attempts = connector->connect_attempts;
  out->activated_links = connector->activated_links;
  out->task_running = connector->task_running;
  out->connected = connector->registered;
  out->closing = connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_CLOSING ||
                 connector->state == FLOWIE_CLUSTER_PEER_CONNECTOR_DRAINED;
  out->terminal_error = connector->terminal_error;
  out->last_error = connector->last_error;
  turbo_mutex_unlock(&connector->mutex);
  return TURBO_OK;
}

int flowie_cluster_peer_connector_destroy(flowie_cluster_peer_connector_t *connector) {
  if (!connector) return TURBO_EINVAL;
  turbo_mutex_lock(&connector->mutex);
  if (connector->state != FLOWIE_CLUSTER_PEER_CONNECTOR_DRAINED || connector->task_running ||
      connector->registered || connector->link || connector->socket) {
    turbo_mutex_unlock(&connector->mutex);
    return TURBO_EBUSY;
  }
  turbo_mutex_unlock(&connector->mutex);
  turbo_cond_destroy(&connector->changed);
  turbo_mutex_destroy(&connector->mutex);
  tstr_free(connector->remote_host);
  tstr_free(connector->cluster_id);
  tstr_free(connector->local_node_id);
  tstr_free(connector->remote_node_id);
  tstr_free(connector->ca_file);
  tstr_free(connector->cert_file);
  tstr_free(connector->key_file);
  tstr_free(connector->key_password);
  free(connector);
  return TURBO_OK;
}
