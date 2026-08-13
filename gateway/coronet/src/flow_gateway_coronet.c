#include "turbo_flow_gateway_coronet.h"

#include "flow_coronet_execution.h"
#include "flow_coronet_runtime.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <stdlib.h>
#include <string.h>

typedef struct flow_gateway_coronet_session_s {
  int in_use;
  int runtime_open;
  uint64_t session_id;
  uint64_t generation;
  uint64_t pending_delivery_id;
  coro_socket_t *socket;
} flow_gateway_coronet_session_t;

struct turbo_flow_gateway_coronet_server_s {
  tf_coronet_execution_t execution;
  turbo_flow_gateway_t *gateway;
  turbo_flow_gateway_info_t gateway_info;
  turbo_flow_gateway_coronet_transport_t transport;
  turbo_flow_gateway_runtime_t *runtime;
  turbo_flow_gateway_runtime_config_t runtime_config;
  turbo_flow_gateway_runtime_ops_t user_ops;
  void *runtime_ctx;
  turbo_flow_gateway_coronet_identity_fn resolve_identity;
  void *identity_ctx;
  flow_gateway_coronet_session_t *sessions;
  coro_socket_t *listener;
  uint64_t next_session_id;
  uint64_t socket_timeout_ms;
  int port;
  int started;
  int stopping;
  int network_stopped;
  int execution_started;
  char host[TURBO_FLOW_GATEWAY_CORONET_HOST_MAX + 1u];
  char path[TURBO_FLOW_GATEWAY_CORONET_PATH_MAX + 1u];
  char subprotocol[TURBO_FLOW_GATEWAY_CORONET_SUBPROTOCOL_MAX + 1u];
  char cert_file[TURBO_FLOW_GATEWAY_CORONET_TLS_PATH_MAX + 1u];
  char key_file[TURBO_FLOW_GATEWAY_CORONET_TLS_PATH_MAX + 1u];
  char key_password[TURBO_FLOW_GATEWAY_CORONET_TLS_PATH_MAX + 1u];
  char ca_file[TURBO_FLOW_GATEWAY_CORONET_TLS_PATH_MAX + 1u];
  char cipher_list[TURBO_FLOW_GATEWAY_CORONET_TLS_PATH_MAX + 1u];
  turbo_tls_client_auth_t client_auth;
};

typedef struct flow_gateway_coronet_settle_call_s {
  turbo_flow_gateway_coronet_server_t *server;
  uint64_t delivery_id;
  int status;
  turbo_flow_gateway_feed_result_t result;
} flow_gateway_coronet_settle_call_t;

typedef struct flow_gateway_coronet_snapshot_call_s {
  turbo_flow_gateway_coronet_server_t *server;
  turbo_flow_gateway_coronet_snapshot_t *out;
} flow_gateway_coronet_snapshot_call_t;

static int flow_gateway_coronet_text_copy(char *out, size_t capacity, const char *text,
                                          int required) {
  size_t length;
  if (!out || capacity == 0u || (required && (!text || !text[0]))) return TURBO_EINVAL;
  if (!text || !text[0]) {
    out[0] = '\0';
    return TURBO_OK;
  }
  length = strlen(text);
  if (length >= capacity) return TURBO_EMSGSIZE;
  memcpy(out, text, length + 1u);
  return TURBO_OK;
}

static int flow_gateway_coronet_transport_valid(turbo_flow_gateway_coronet_transport_t transport) {
  return transport >= TURBO_FLOW_GATEWAY_CORONET_UDP && transport <= TURBO_FLOW_GATEWAY_CORONET_WSS;
}

static tf_coronet_transport_t
flow_gateway_coronet_transport(turbo_flow_gateway_coronet_transport_t transport) {
  switch (transport) {
  case TURBO_FLOW_GATEWAY_CORONET_UDP:
    return TF_CORONET_TRANSPORT_UDP;
  case TURBO_FLOW_GATEWAY_CORONET_TCP:
    return TF_CORONET_TRANSPORT_TCP;
  case TURBO_FLOW_GATEWAY_CORONET_TLS:
    return TF_CORONET_TRANSPORT_TLS;
  case TURBO_FLOW_GATEWAY_CORONET_WS:
    return TF_CORONET_TRANSPORT_WS;
  case TURBO_FLOW_GATEWAY_CORONET_WSS:
    return TF_CORONET_TRANSPORT_WSS;
  default:
    return TF_CORONET_TRANSPORT_COUNT;
  }
}

static int
flow_gateway_coronet_protocol_transport_valid(turbo_flow_gateway_protocol_t protocol,
                                              turbo_flow_gateway_coronet_transport_t transport,
                                              uint8_t allow_insecure_lwm2m) {
  switch (protocol) {
  case TURBO_FLOW_GATEWAY_PROTOCOL_MQTT_SN:
  case TURBO_FLOW_GATEWAY_PROTOCOL_COAP:
    return transport == TURBO_FLOW_GATEWAY_CORONET_UDP ? TURBO_OK : TURBO_ENOTSUP;
  case TURBO_FLOW_GATEWAY_PROTOCOL_LWM2M:
    if (transport != TURBO_FLOW_GATEWAY_CORONET_UDP) return TURBO_ENOTSUP;
    return allow_insecure_lwm2m ? TURBO_OK : TURBO_EPERM;
  case TURBO_FLOW_GATEWAY_PROTOCOL_OCPP:
    return transport == TURBO_FLOW_GATEWAY_CORONET_WS || transport == TURBO_FLOW_GATEWAY_CORONET_WSS
               ? TURBO_OK
               : TURBO_ENOTSUP;
  case TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960:
  case TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808:
    return transport == TURBO_FLOW_GATEWAY_CORONET_TCP ||
                   transport == TURBO_FLOW_GATEWAY_CORONET_TLS
               ? TURBO_OK
               : TURBO_ENOTSUP;
  default:
    return TURBO_EINVAL;
  }
}

static int flow_gateway_coronet_subprotocol_valid(const turbo_flow_gateway_info_t *info,
                                                  const char *subprotocol) {
  if (!info || info->protocol != TURBO_FLOW_GATEWAY_PROTOCOL_OCPP)
    return (!subprotocol || !subprotocol[0]) ? TURBO_OK : TURBO_EINVAL;
  if (!subprotocol || !subprotocol[0]) return TURBO_EINVAL;
  if (strcmp(info->protocol_version, "1.6J") == 0)
    return strcmp(subprotocol, "ocpp1.6") == 0 ? TURBO_OK : TURBO_ENOTSUP;
  if (strcmp(info->protocol_version, "2.0.1") == 0)
    return strcmp(subprotocol, "ocpp2.0.1") == 0 ? TURBO_OK : TURBO_ENOTSUP;
  return TURBO_ENOTSUP;
}

static int flow_gateway_coronet_tls_validate(const turbo_flow_gateway_coronet_config_t *config) {
  const int secure = config->transport == TURBO_FLOW_GATEWAY_CORONET_TLS ||
                     config->transport == TURBO_FLOW_GATEWAY_CORONET_WSS;
  if (!secure)
    return (!config->tls.cert_file && !config->tls.key_file && !config->tls.ca_file &&
            !config->tls.key_password && !config->tls.cipher_list)
               ? TURBO_OK
               : TURBO_EINVAL;
  if (config->tls.size < sizeof(config->tls) ||
      config->tls.abi_version != TURBO_FLOW_GATEWAY_CORONET_ABI_VERSION || !config->tls.cert_file ||
      !config->tls.cert_file[0] || !config->tls.key_file || !config->tls.key_file[0])
    return TURBO_EINVAL;
  if (config->tls.client_auth != TURBO_TLS_CLIENT_AUTH_NONE &&
      config->tls.client_auth != TURBO_TLS_CLIENT_AUTH_REQUIRED)
    return TURBO_EINVAL;
  if (config->tls.client_auth == TURBO_TLS_CLIENT_AUTH_REQUIRED &&
      (!config->tls.ca_file || !config->tls.ca_file[0]))
    return TURBO_EINVAL;
  return TURBO_OK;
}

static flow_gateway_coronet_session_t *
flow_gateway_coronet_session_find(turbo_flow_gateway_coronet_server_t *server,
                                  uint64_t session_id) {
  for (size_t i = 0u; i < server->runtime_config.max_sessions; ++i) {
    if (server->sessions[i].in_use && server->sessions[i].session_id == session_id)
      return &server->sessions[i];
  }
  return NULL;
}

static flow_gateway_coronet_session_t *
flow_gateway_coronet_session_acquire(turbo_flow_gateway_coronet_server_t *server,
                                     coro_socket_t *socket) {
  flow_gateway_coronet_session_t *entry = NULL;
  for (size_t i = 0u; i < server->runtime_config.max_sessions; ++i) {
    if (!server->sessions[i].in_use) {
      entry = &server->sessions[i];
      break;
    }
  }
  if (!entry || server->next_session_id == UINT64_MAX) return NULL;
  memset(entry, 0, sizeof(*entry));
  entry->in_use = 1;
  entry->session_id = ++server->next_session_id;
  entry->generation = 1u;
  entry->socket = socket;
  return entry;
}

static void flow_gateway_coronet_session_release(flow_gateway_coronet_session_t *entry) {
  if (entry) memset(entry, 0, sizeof(*entry));
}

static int flow_gateway_coronet_publish(void *ctx,
                                        const turbo_flow_gateway_publish_request_t *request,
                                        turbo_flow_gateway_publish_disposition_t *disposition) {
  turbo_flow_gateway_coronet_server_t *server = (turbo_flow_gateway_coronet_server_t *)ctx;
  flow_gateway_coronet_session_t *entry;
  int rc;
  if (!server || !request || !disposition) return TURBO_EINVAL;
  entry = flow_gateway_coronet_session_find(server, request->session_id);
  if (!entry || entry->generation != request->session_generation) return TURBO_EPROTO;
  rc = server->user_ops.publish(server->runtime_ctx, request, disposition);
  if (rc == TURBO_OK && *disposition == TURBO_FLOW_GATEWAY_PUBLISH_PENDING)
    entry->pending_delivery_id = request->delivery_id;
  return rc;
}

static void flow_gateway_coronet_settled(void *ctx, uint64_t session_id, uint64_t generation,
                                         uint64_t delivery_id,
                                         const turbo_flow_gateway_metadata_t *metadata,
                                         int status) {
  turbo_flow_gateway_coronet_server_t *server = (turbo_flow_gateway_coronet_server_t *)ctx;
  flow_gateway_coronet_session_t *entry = flow_gateway_coronet_session_find(server, session_id);
  if (entry && entry->generation == generation && entry->pending_delivery_id == delivery_id)
    entry->pending_delivery_id = 0u;
  if (server->user_ops.settled)
    server->user_ops.settled(server->runtime_ctx, session_id, generation, delivery_id, metadata,
                             status);
}

static int flow_gateway_coronet_reply(
    void *ctx, uint64_t session_id, uint64_t generation,
    uint64_t delivery_id, const turbo_flow_gateway_frame_output_t *frame) {
  turbo_flow_gateway_coronet_server_t *server =
      (turbo_flow_gateway_coronet_server_t *)ctx;
  flow_gateway_coronet_session_t *entry;
  (void)delivery_id;
  if (!server || !frame || !frame->data || frame->data_size == 0u)
    return TURBO_EINVAL;
  entry = flow_gateway_coronet_session_find(server, session_id);
  if (!entry || entry->generation != generation || !entry->socket)
    return TURBO_EPROTO;
  return coro_socket_send(entry->socket, (const char *)frame->data,
                          frame->data_size);
}

static void flow_gateway_coronet_session_closed(void *ctx, uint64_t session_id, uint64_t generation,
                                                int status) {
  turbo_flow_gateway_coronet_server_t *server = (turbo_flow_gateway_coronet_server_t *)ctx;
  flow_gateway_coronet_session_t *entry = flow_gateway_coronet_session_find(server, session_id);
  if (entry && entry->generation == generation) entry->runtime_open = 0;
  if (server->user_ops.session_closed)
    server->user_ops.session_closed(server->runtime_ctx, session_id, generation, status);
}

static int flow_gateway_coronet_resolve_identity(turbo_flow_gateway_coronet_server_t *server,
                                                 flow_gateway_coronet_session_t *entry,
                                                 char *device_id, size_t capacity) {
  turbo_flow_gateway_coronet_identity_request_t request =
      TURBO_FLOW_GATEWAY_CORONET_IDENTITY_REQUEST_INIT;
  if (!server->resolve_identity) {
    if (server->gateway_info.protocol == TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960 ||
        server->gateway_info.protocol == TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808) {
      device_id[0] = '\0';
      return TURBO_OK;
    }
    return TURBO_EPERM;
  }
  request.protocol = server->gateway_info.protocol;
  request.transport = server->transport;
  request.session_id = entry->session_id;
  request.generation = entry->generation;
  request.socket = entry->socket;
  device_id[0] = '\0';
  {
    int rc = server->resolve_identity(server->identity_ctx, &request, device_id, capacity);
    if (rc != TURBO_OK) return rc;
  }
  if (!device_id[0]) return TURBO_EPERM;
  return TURBO_OK;
}

static void flow_gateway_coronet_handler(coro_socket_t *client, void *arg) {
  turbo_flow_gateway_coronet_server_t *server = (turbo_flow_gateway_coronet_server_t *)arg;
  flow_gateway_coronet_session_t *entry;
  turbo_flow_gateway_session_open_request_t open = TURBO_FLOW_GATEWAY_SESSION_OPEN_REQUEST_INIT;
  char device_id[TURBO_FLOW_GATEWAY_DEVICE_ID_MAX + 1u];
  int status = TURBO_OK;
  if (!client || !server || server->stopping) return;
  entry = flow_gateway_coronet_session_acquire(server, client);
  if (!entry) return;
  status = flow_gateway_coronet_resolve_identity(server, entry, device_id, sizeof(device_id));
  if (status != TURBO_OK) goto done;
  open.session_id = entry->session_id;
  open.generation = entry->generation;
  open.device_id = device_id[0] ? device_id : NULL;
  open.protocol_version = server->gateway_info.protocol_version;
  status = turbo_flow_gateway_runtime_session_open(server->runtime, &open);
  if (status != TURBO_OK) goto done;
  entry->runtime_open = 1;
  for (;;) {
    turbo_flow_gateway_feed_result_t result = TURBO_FLOW_GATEWAY_FEED_RESULT_INIT;
    char *data = NULL;
    size_t size = 0u;
    /*
     * A pending adapter/storage settlement owns both the runtime frame and the
     * transport needed to emit its protocol reply. Stopping closes admission,
     * but must not let the accepted handler release that transport first.
     */
    while (entry->pending_delivery_id != 0u && entry->runtime_open)
      coro_sleep(server->execution.context, 1u);
    if (server->stopping || !entry->runtime_open) break;
    status = coro_socket_recv(client, &data, &size);
    if (status != TURBO_OK) {
      if (data) coro_socket_free_recv(data);
      break;
    }
    if (!data || size == 0u) {
      if (data) coro_socket_free_recv(data);
      continue;
    }
    status = turbo_flow_gateway_runtime_session_feed(server->runtime, entry->session_id,
                                                     entry->generation, (const uint8_t *)data, size,
                                                     &result);
    coro_socket_free_recv(data);
    if (status != TURBO_OK) break;
  }

done:
  if (entry->runtime_open && !server->stopping)
    (void)turbo_flow_gateway_runtime_session_close(server->runtime, entry->session_id,
                                                   entry->generation,
                                                   status == TURBO_OK ? TURBO_EOF : status);
  flow_gateway_coronet_session_release(entry);
}

static int flow_gateway_coronet_start_on_loop(void *arg) {
  turbo_flow_gateway_coronet_server_t *server = (turbo_flow_gateway_coronet_server_t *)arg;
  tf_coronet_transport_t transport = flow_gateway_coronet_transport(server->transport);
  int rc;
  server->listener = tf_coronet_create_server_socket(server->execution.context, transport);
  if (!server->listener) return TURBO_ENOTSUP;
  coro_socket_set_timeout(server->listener, server->socket_timeout_ms);
  if (server->transport == TURBO_FLOW_GATEWAY_CORONET_TLS ||
      server->transport == TURBO_FLOW_GATEWAY_CORONET_WSS) {
    turbo_tls_server_config_t tls = {sizeof(tls),
                                     server->cert_file,
                                     server->key_file,
                                     server->key_password[0] ? server->key_password : NULL,
                                     server->ca_file[0] ? server->ca_file : NULL,
                                     server->cipher_list[0] ? server->cipher_list : NULL,
                                     server->client_auth};
    rc = coro_socket_set_tls_server_config(server->listener, &tls);
    if (rc != TURBO_OK) goto fail;
  }
  if (server->transport == TURBO_FLOW_GATEWAY_CORONET_WS ||
      server->transport == TURBO_FLOW_GATEWAY_CORONET_WSS) {
    coro_ws_server_config_t ws = CORO_WS_SERVER_CONFIG_DEFAULT;
    ws.path = server->path[0] ? server->path : NULL;
    ws.subprotocol = server->subprotocol[0] ? server->subprotocol : NULL;
    ws.max_message_size = server->runtime_config.max_frame_size;
    ws.binary_only = 0;
    rc = coro_socket_set_ws_server_config(server->listener, &ws);
    if (rc != TURBO_OK) goto fail;
  }
  rc = tf_coronet_listen_socket(server->listener, transport, server->host, server->port,
                                server->path[0] ? server->path : NULL, flow_gateway_coronet_handler,
                                server);
  if (rc != TURBO_OK) goto fail;
  server->started = 1;
  server->network_stopped = 0;
  return TURBO_OK;

fail:
  coro_socket_destroy(server->listener);
  server->listener = NULL;
  return rc;
}

static int flow_gateway_coronet_begin_on_loop(void *arg) {
  turbo_flow_gateway_coronet_server_t *server = (turbo_flow_gateway_coronet_server_t *)arg;
  if (server->stopping) return TURBO_OK;
  server->stopping = 1;
  (void)turbo_flow_gateway_runtime_begin_shutdown(server->runtime);
  return server->listener
             ? coro_socket_server_close_admission(server->listener)
             : TURBO_OK;
}

static int flow_gateway_coronet_shutdown_state_on_loop(void *arg) {
  turbo_flow_gateway_coronet_server_t *server = (turbo_flow_gateway_coronet_server_t *)arg;
  turbo_flow_gateway_runtime_snapshot_t snapshot = TURBO_FLOW_GATEWAY_RUNTIME_SNAPSHOT_INIT;
  int rc;
  if (server->listener && !coro_socket_server_is_stopped(server->listener)) return TURBO_EBUSY;
  rc = turbo_flow_gateway_runtime_snapshot(server->runtime, &snapshot);
  if (rc != TURBO_OK) return rc;
  if (snapshot.active_sessions != 0u || snapshot.pending_settlements != 0u) return TURBO_EBUSY;
  if (server->listener) {
    coro_socket_destroy(server->listener);
    server->listener = NULL;
  }
  server->network_stopped = 1;
  return TURBO_OK;
}

static int flow_gateway_coronet_force_on_loop(void *arg) {
  flow_gateway_coronet_settle_call_t *call = (flow_gateway_coronet_settle_call_t *)arg;
  turbo_flow_gateway_coronet_server_t *server = call->server;
  server->stopping = 1;
  if (server->listener) (void)coro_socket_server_stop(server->listener);
  return turbo_flow_gateway_runtime_force_shutdown(server->runtime, call->status);
}

static int flow_gateway_coronet_settle_on_loop(void *arg) {
  flow_gateway_coronet_settle_call_t *call = (flow_gateway_coronet_settle_call_t *)arg;
  return turbo_flow_gateway_runtime_settle(call->server->runtime, call->delivery_id, call->status,
                                           &call->result);
}

static int flow_gateway_coronet_snapshot_on_loop(void *arg) {
  flow_gateway_coronet_snapshot_call_t *call = (flow_gateway_coronet_snapshot_call_t *)arg;
  turbo_flow_gateway_coronet_snapshot_t *out = call->out;
  turbo_flow_gateway_coronet_server_t *server = call->server;
  turbo_flow_gateway_coronet_snapshot_t snapshot = TURBO_FLOW_GATEWAY_CORONET_SNAPSHOT_INIT;
  int rc = turbo_flow_gateway_runtime_snapshot(server->runtime, &snapshot.runtime);
  if (rc != TURBO_OK) return rc;
  snapshot.started = server->started ? 1u : 0u;
  snapshot.stopping = server->stopping ? 1u : 0u;
  snapshot.network_stopped = server->network_stopped ? 1u : 0u;
  for (size_t i = 0u; i < server->runtime_config.max_sessions; ++i)
    if (server->sessions[i].in_use) snapshot.active_handlers++;
  *out = snapshot;
  return TURBO_OK;
}

int turbo_flow_gateway_coronet_server_create(const turbo_flow_gateway_coronet_config_t *config,
                                             turbo_flow_gateway_coronet_server_t **out) {
  turbo_flow_gateway_coronet_server_t *server;
  turbo_flow_gateway_runtime_ops_t runtime_ops = TURBO_FLOW_GATEWAY_RUNTIME_OPS_INIT;
  turbo_flow_gateway_info_t info = TURBO_FLOW_GATEWAY_INFO_INIT;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != TURBO_FLOW_GATEWAY_CORONET_ABI_VERSION || !config->gateway ||
      !flow_gateway_coronet_transport_valid(config->transport) || !config->host ||
      !config->host[0] || config->port < 0 || config->port > 65535 ||
      config->socket_timeout_ms == 0u || config->runtime.size < sizeof(config->runtime) ||
      config->runtime.abi_version != TURBO_FLOW_GATEWAY_RUNTIME_ABI_VERSION ||
      config->runtime.max_sessions == 0u || !config->runtime_ops.publish ||
      config->runtime_ops.reply || !out)
    return TURBO_EINVAL;
  rc = turbo_flow_gateway_get_info(config->gateway, &info);
  if (rc != TURBO_OK) return rc;
  rc = flow_gateway_coronet_protocol_transport_valid(info.protocol, config->transport,
                                                     config->allow_insecure_lwm2m);
  if (rc != TURBO_OK) return rc;
  rc = flow_gateway_coronet_subprotocol_valid(&info, config->subprotocol);
  if (rc != TURBO_OK) return rc;
  rc = flow_gateway_coronet_tls_validate(config);
  if (rc != TURBO_OK) return rc;
  if ((config->transport == TURBO_FLOW_GATEWAY_CORONET_WS ||
       config->transport == TURBO_FLOW_GATEWAY_CORONET_WSS) &&
      config->path && config->path[0] && config->path[0] != '/')
    return TURBO_EINVAL;
  if (!config->resolve_identity && info.protocol != TURBO_FLOW_GATEWAY_PROTOCOL_GBT_32960 &&
      info.protocol != TURBO_FLOW_GATEWAY_PROTOCOL_JTT_808)
    return TURBO_EINVAL;
  server = (turbo_flow_gateway_coronet_server_t *)calloc(1u, sizeof(*server));
  if (!server) return TURBO_ENOMEM;
  server->sessions = (flow_gateway_coronet_session_t *)calloc(config->runtime.max_sessions,
                                                              sizeof(*server->sessions));
  if (!server->sessions) {
    free(server);
    return TURBO_ENOMEM;
  }
  server->gateway = config->gateway;
  server->gateway_info = info;
  server->transport = config->transport;
  server->runtime_config = config->runtime;
  server->user_ops = config->runtime_ops;
  server->runtime_ctx = config->runtime_ctx;
  server->resolve_identity = config->resolve_identity;
  server->identity_ctx = config->identity_ctx;
  server->socket_timeout_ms = config->socket_timeout_ms;
  server->port = config->port;
  server->client_auth = config->tls.client_auth;
  rc = flow_gateway_coronet_text_copy(server->host, sizeof(server->host), config->host, 1);
  if (rc == TURBO_OK)
    rc = flow_gateway_coronet_text_copy(server->path, sizeof(server->path), config->path, 0);
  if (rc == TURBO_OK)
    rc = flow_gateway_coronet_text_copy(server->subprotocol, sizeof(server->subprotocol),
                                        config->subprotocol, 0);
  if (rc == TURBO_OK)
    rc = flow_gateway_coronet_text_copy(server->cert_file, sizeof(server->cert_file),
                                        config->tls.cert_file, 0);
  if (rc == TURBO_OK)
    rc = flow_gateway_coronet_text_copy(server->key_file, sizeof(server->key_file),
                                        config->tls.key_file, 0);
  if (rc == TURBO_OK)
    rc = flow_gateway_coronet_text_copy(server->key_password, sizeof(server->key_password),
                                        config->tls.key_password, 0);
  if (rc == TURBO_OK)
    rc = flow_gateway_coronet_text_copy(server->ca_file, sizeof(server->ca_file),
                                        config->tls.ca_file, 0);
  if (rc == TURBO_OK)
    rc = flow_gateway_coronet_text_copy(server->cipher_list, sizeof(server->cipher_list),
                                        config->tls.cipher_list, 0);
  if (rc != TURBO_OK) goto fail;
  rc = tf_coronet_execution_init(&server->execution, &config->execution);
  if (rc != TURBO_OK) goto fail;
  runtime_ops.publish = flow_gateway_coronet_publish;
  runtime_ops.settled = flow_gateway_coronet_settled;
  runtime_ops.session_closed = flow_gateway_coronet_session_closed;
  runtime_ops.reply = flow_gateway_coronet_reply;
  rc = turbo_flow_gateway_runtime_create(server->gateway, &server->runtime_config, &runtime_ops,
                                         server, &server->runtime);
  if (rc != TURBO_OK) {
    tf_coronet_execution_destroy(&server->execution);
    goto fail;
  }
  *out = server;
  return TURBO_OK;

fail:
  free(server->sessions);
  free(server);
  return rc;
}

int turbo_flow_gateway_coronet_server_start(turbo_flow_gateway_coronet_server_t *server) {
  int rc;
  if (!server) return TURBO_EINVAL;
  if (server->started) return TURBO_EALREADY;
  rc = tf_coronet_execution_start(&server->execution);
  if (rc != TURBO_OK) return rc;
  server->execution_started = 1;
  rc = tf_coronet_execution_call(&server->execution, flow_gateway_coronet_start_on_loop, server,
                                 TURBO_FLOW_GATEWAY_CORONET_CALL_TIMEOUT_NS);
  if (rc != TURBO_OK) {
    tf_coronet_execution_stop(&server->execution);
    server->execution_started = 0;
  }
  return rc;
}

int turbo_flow_gateway_coronet_server_begin_shutdown(turbo_flow_gateway_coronet_server_t *server) {
  if (!server || !server->started) return TURBO_EINVAL;
  return tf_coronet_execution_call(&server->execution, flow_gateway_coronet_begin_on_loop, server,
                                   TURBO_FLOW_GATEWAY_CORONET_CALL_TIMEOUT_NS);
}

int turbo_flow_gateway_coronet_server_wait_shutdown(turbo_flow_gateway_coronet_server_t *server,
                                                    uint64_t timeout_ms) {
  uint64_t started_at;
  uint64_t deadline;
  if (!server || !server->started || !server->stopping || timeout_ms == 0u) return TURBO_EINVAL;
  started_at = turbo_monotonic_ms();
  if (timeout_ms > UINT64_MAX - started_at) return TURBO_ERANGE;
  deadline = started_at + timeout_ms;
  do {
    int rc =
        tf_coronet_execution_call(&server->execution, flow_gateway_coronet_shutdown_state_on_loop,
                                  server, TURBO_FLOW_GATEWAY_CORONET_CALL_TIMEOUT_NS);
    if (rc == TURBO_OK) return TURBO_OK;
    if (rc != TURBO_EBUSY) return rc;
    turbo_sleep_ms(1u);
  } while (turbo_monotonic_ms() < deadline);
  return TURBO_ETIMEDOUT;
}

int turbo_flow_gateway_coronet_server_force_shutdown(turbo_flow_gateway_coronet_server_t *server,
                                                     int status, uint64_t timeout_ms) {
  flow_gateway_coronet_settle_call_t call;
  int rc;
  if (!server || !server->started || status == TURBO_OK || timeout_ms == 0u) return TURBO_EINVAL;
  memset(&call, 0, sizeof(call));
  call.server = server;
  call.status = status;
  rc = tf_coronet_execution_call(&server->execution, flow_gateway_coronet_force_on_loop, &call,
                                 TURBO_FLOW_GATEWAY_CORONET_CALL_TIMEOUT_NS);
  if (rc != TURBO_OK) return rc;
  return turbo_flow_gateway_coronet_server_wait_shutdown(server, timeout_ms);
}

int turbo_flow_gateway_coronet_server_settle(turbo_flow_gateway_coronet_server_t *server,
                                             uint64_t delivery_id, int status,
                                             turbo_flow_gateway_feed_result_t *result) {
  flow_gateway_coronet_settle_call_t call;
  int rc;
  if (!server || !server->execution_started || delivery_id == 0u || !result ||
      result->size < sizeof(*result) ||
      result->abi_version != TURBO_FLOW_GATEWAY_RUNTIME_ABI_VERSION)
    return TURBO_EINVAL;
  memset(&call, 0, sizeof(call));
  call.server = server;
  call.delivery_id = delivery_id;
  call.status = status;
  call.result = (turbo_flow_gateway_feed_result_t)TURBO_FLOW_GATEWAY_FEED_RESULT_INIT;
  rc = tf_coronet_execution_call_coro(
      &server->execution, flow_gateway_coronet_settle_on_loop, &call,
      TURBO_FLOW_GATEWAY_CORONET_CALL_TIMEOUT_NS);
  if (rc == TURBO_OK) *result = call.result;
  return rc;
}

int turbo_flow_gateway_coronet_server_snapshot(turbo_flow_gateway_coronet_server_t *server,
                                               turbo_flow_gateway_coronet_snapshot_t *out) {
  flow_gateway_coronet_snapshot_call_t call;
  if (!server || !out || out->size < sizeof(*out) ||
      out->abi_version != TURBO_FLOW_GATEWAY_CORONET_ABI_VERSION)
    return TURBO_EINVAL;
  if (!server->execution_started) {
    turbo_flow_gateway_coronet_snapshot_t snapshot = TURBO_FLOW_GATEWAY_CORONET_SNAPSHOT_INIT;
    int rc = turbo_flow_gateway_runtime_snapshot(server->runtime, &snapshot.runtime);
    if (rc != TURBO_OK) return rc;
    *out = snapshot;
    return TURBO_OK;
  }
  call.server = server;
  call.out = out;
  return tf_coronet_execution_call(&server->execution, flow_gateway_coronet_snapshot_on_loop, &call,
                                   TURBO_FLOW_GATEWAY_CORONET_CALL_TIMEOUT_NS);
}

int turbo_flow_gateway_coronet_server_destroy(turbo_flow_gateway_coronet_server_t *server) {
  int rc;
  if (!server) return TURBO_OK;
  if (server->started && !server->network_stopped) return TURBO_EBUSY;
  if (!server->started) (void)turbo_flow_gateway_runtime_begin_shutdown(server->runtime);
  rc = turbo_flow_gateway_runtime_destroy(server->runtime);
  if (rc != TURBO_OK) return rc;
  server->runtime = NULL;
  tf_coronet_execution_destroy(&server->execution);
  free(server->sessions);
  free(server);
  return TURBO_OK;
}
