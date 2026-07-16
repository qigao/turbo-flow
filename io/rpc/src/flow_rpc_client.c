#include "turbo_flow_rpc.h"

#include "flow_connection.h"
#include "flow_timer.h"
#include "http/http_client.h"
#include "rpc_client.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef struct flow_rpc_client_adapter_s {
  turbo_flow_t *flow;
  tstr_t source_name;
  tstr_t url;
  tstr_t method;
  tstr_t bearer_token;
  tstr_t poll_params;
  uint32_t poll_interval_ms;
  http_client_t *http_client;
  rpc_client_t *rpc_client;
  atomic_int started;
  tf_connection_state_t connection;
  tf_timer_t wait_timer;
  int wait_timer_initialized;
  turbo_mutex_t lock;
  int lock_initialized;
  int poll_thread_started;
  turbo_thread_t poll_thread;
} flow_rpc_client_adapter_t;

static int flow_rpc_client_wait_for_ms(flow_rpc_client_adapter_t *adapter, uint32_t delay_ms) {
  int rc;
  if (!adapter) return TURBO_EINVAL;
  if (delay_ms == 0)
    return atomic_load_explicit(&adapter->started, memory_order_acquire) ? TURBO_OK
                                                                         : TURBO_ESHUTDOWN;
  if (!adapter->wait_timer_initialized) return TURBO_ESHUTDOWN;
  rc = tf_timer_wait_for_ms(&adapter->wait_timer, delay_ms);
  if (rc == TURBO_ETIMEDOUT) return TURBO_OK;
  if (atomic_load_explicit(&adapter->started, memory_order_acquire)) return rc;
  return TURBO_ESHUTDOWN;
}

static const turbo_flow_option_field_t FLOW_RPC_CLIENT_FIELDS[] = {
    {"url", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"method", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"bearer_token", TURBO_FLOW_OPTION_SECRET, TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"poll_params", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"poll_interval_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_RPC_CLIENT_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_RPC,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
    FLOW_RPC_CLIENT_FIELDS,
    sizeof(FLOW_RPC_CLIENT_FIELDS) / sizeof(FLOW_RPC_CLIENT_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_RPC_POLL_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_RPC,
    TURBO_FLOW_ADAPTER_SOURCE,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_RPC_CLIENT_FIELDS,
    sizeof(FLOW_RPC_CLIENT_FIELDS) / sizeof(FLOW_RPC_CLIENT_FIELDS[0])};

static int flow_rpc_client_replace_payload(turbo_flow_msg_t *msg, const char *data, size_t len) {
  tstr_t payload;
  if (!msg || (len > 0 && !data)) return TURBO_EINVAL;
  payload = tstr_new_len(data ? data : "", len);
  if (!payload) return TURBO_ENOMEM;
  turbo_flow_msg_clear_content(msg);
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  msg->buffer = NULL;
  msg->owned_payload = payload;
  msg->payload = tstr_to_v(payload);
  return TURBO_OK;
}

static void flow_rpc_poll_thread(void *arg) {
  flow_rpc_client_adapter_t *adapter = (flow_rpc_client_adapter_t *)arg;
  if (!adapter) return;
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    if (flow_rpc_client_wait_for_ms(adapter, 1) != TURBO_OK) return;
  }
  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    rpc_call_result_t result;
    turbo_flow_msg_t msg;
    int call_rc;
    int publish_rc;
    memset(&result, 0, sizeof(result));
    if (tf_connection_request_begin(&adapter->connection,
                                    adapter->poll_params ? tstr_len(adapter->poll_params) : 0u) !=
        TURBO_OK) {
      break;
    }
    turbo_mutex_lock(&adapter->lock);
    call_rc = rpc_client_call(adapter->rpc_client, adapter->method,
                              adapter->poll_params ? adapter->poll_params : "null", &result);
    turbo_flow_msg_init(&msg);
    if (call_rc == 0 && result.success && result.result) {
      msg.owned_payload = tstr_dup(result.result);
      msg.status = result.http_status;
    } else {
      msg.owned_payload = tstr_dup("");
      msg.status =
          call_rc != 0 ? TURBO_EIO : (result.http_status != 0 ? result.http_status : TURBO_EPROTO);
    }
    rpc_result_free(&result);
    turbo_mutex_unlock(&adapter->lock);
    atomic_store_explicit(&adapter->connection.last_status,
                          call_rc == 0 && msg.owned_payload ? TURBO_OK : TURBO_EIO,
                          memory_order_relaxed);
    (void)tf_connection_request_end(&adapter->connection,
                                    adapter->poll_params ? tstr_len(adapter->poll_params) : 0u);
    if (!msg.owned_payload) break;
    msg.payload = tstr_to_v(msg.owned_payload);
    publish_rc = turbo_flow_publish(adapter->flow, adapter->source_name, &msg);
    turbo_flow_msg_cleanup(&msg);
    if (publish_rc != TURBO_OK) break;
    if (flow_rpc_client_wait_for_ms(adapter, adapter->poll_interval_ms) != TURBO_OK) break;
  }
}

static int flow_rpc_client_start(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flow_rpc_client_adapter_t *adapter = (flow_rpc_client_adapter_t *)ctx;
  if (!adapter || !stage || (adapter->poll_interval_ms > 0 && !stage->is_source) ||
      (adapter->poll_interval_ms == 0 && stage->is_source)) {
    return TURBO_EINVAL;
  }
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_OK);
  if (rpc_client_connect(adapter->rpc_client) != 0) {
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_EIO);
    return TURBO_EIO;
  }
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
  if (adapter->poll_interval_ms > 0) {
    adapter->flow = flow;
    tstr_freep(&adapter->source_name);
    adapter->source_name = tstr_dup(stage->name);
    if (!adapter->source_name) {
      atomic_store_explicit(&adapter->started, 0, memory_order_release);
      rpc_client_disconnect(adapter->rpc_client);
      tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
      tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_ENOMEM);
      return TURBO_ENOMEM;
    }
    if (turbo_thread_create(&adapter->poll_thread, flow_rpc_poll_thread, adapter) != TURBO_OK) {
      tstr_freep(&adapter->source_name);
      atomic_store_explicit(&adapter->started, 0, memory_order_release);
      rpc_client_disconnect(adapter->rpc_client);
      tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
      tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_EINVAL);
      return TURBO_EINVAL;
    }
    adapter->poll_thread_started = 1;
  }
  return TURBO_OK;
}

static int flow_rpc_client_consume(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_rpc_client_adapter_t *adapter = (flow_rpc_client_adapter_t *)ctx;
  rpc_call_result_t result;
  char *params;
  size_t request_len;
  int call_rc;
  int rc;
  (void)flow;
  (void)stage;
  if (!adapter || !msg || !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    return TURBO_EINVAL;
  }
  if (adapter->poll_interval_ms > 0) return TURBO_EINVAL;
  request_len = msg->payload.len;
  params = (char *)malloc(msg->payload.len + 1);
  if (!params) return TURBO_ENOMEM;
  if (msg->payload.len > 0) memcpy(params, msg->payload.data, msg->payload.len);
  params[msg->payload.len] = '\0';
  memset(&result, 0, sizeof(result));
  rc = tf_connection_request_begin(&adapter->connection, request_len);
  if (rc != TURBO_OK) {
    free(params);
    return rc;
  }
  turbo_mutex_lock(&adapter->lock);
  call_rc = rpc_client_call(adapter->rpc_client, adapter->method, params, &result);
  free(params);
  if (call_rc != 0 || !result.success || !result.result) rc = TURBO_EPROTO;
  else {
    msg->status = result.http_status;
    rc = flow_rpc_client_replace_payload(msg, result.result, strlen(result.result));
  }
  rpc_result_free(&result);
  turbo_mutex_unlock(&adapter->lock);
  (void)tf_connection_request_end(&adapter->connection, request_len);
  atomic_store_explicit(&adapter->connection.last_status, rc, memory_order_relaxed);
  return rc;
}

static void flow_rpc_client_stop(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flow_rpc_client_adapter_t *adapter = (flow_rpc_client_adapter_t *)ctx;
  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_OK);
  if (adapter->wait_timer_initialized) tf_timer_signal(&adapter->wait_timer);
  if (adapter->rpc_client) {
    rpc_client_disconnect(adapter->rpc_client);
  }
  if (adapter->poll_thread_started) {
    (void)turbo_thread_join(&adapter->poll_thread);
    adapter->poll_thread_started = 0;
  }
  tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_STOPPED, TURBO_ESHUTDOWN);
}

static int flow_rpc_client_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flow_rpc_client_adapter_t *adapter = (flow_rpc_client_adapter_t *)ctx;
  return adapter ? tf_connection_snapshot(&adapter->connection, out) : TURBO_EINVAL;
}

static void flow_rpc_client_shutdown(void *ctx) {
  flow_rpc_client_adapter_t *adapter = (flow_rpc_client_adapter_t *)ctx;
  if (!adapter) return;
  flow_rpc_client_stop(adapter, NULL, NULL);
  if (adapter->wait_timer_initialized) {
    tf_timer_destroy(&adapter->wait_timer);
    adapter->wait_timer_initialized = 0;
  }
  rpc_client_destroy(adapter->rpc_client);
  http_client_destroy(adapter->http_client);
  if (adapter->lock_initialized) turbo_mutex_destroy(&adapter->lock);
  tstr_freep(&adapter->url);
  tstr_freep(&adapter->method);
  tstr_freep(&adapter->bearer_token);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->poll_params);
  free(adapter);
}

int turbo_flow_rpc_register_client_adapter(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_rpc_client_config_t *config) {
  flow_rpc_client_adapter_t *adapter;
  rpc_client_config_t rpc_config;
  turbo_flow_adapter_ops_t ops;
  int rc;
  if (!flow || !name || name[0] == '\0' || !config || !config->url || config->url[0] == '\0' ||
      !config->method || config->method[0] == '\0' || config->timeout_ms < 0) {
    return TURBO_EINVAL;
  }
  adapter = (flow_rpc_client_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  rc = tf_connection_init(&adapter->connection, config->url, 1u);
  if (rc != TURBO_OK) {
    free(adapter);
    return rc;
  }
  adapter->url = tstr_dup(config->url);
  adapter->method = tstr_dup(config->method);
  adapter->bearer_token = config->bearer_token ? tstr_dup(config->bearer_token) : NULL;
  adapter->poll_params = config->poll_params ? tstr_dup(config->poll_params) : NULL;
  adapter->poll_interval_ms = config->poll_interval_ms;
  if (!adapter->url || !adapter->method || (config->bearer_token && !adapter->bearer_token) ||
      (config->poll_params && !adapter->poll_params)) {
    flow_rpc_client_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  adapter->http_client = http_client_create(NULL);
  if (!adapter->http_client) {
    flow_rpc_client_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  if (tf_timer_init(&adapter->wait_timer) != TURBO_OK) {
    flow_rpc_client_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  adapter->wait_timer_initialized = 1;
  if (config->timeout_ms > 0) http_client_set_timeout(adapter->http_client, config->timeout_ms);
  if (adapter->bearer_token) {
    http_client_set_bearer_token(adapter->http_client, adapter->bearer_token);
  }
  rpc_config.url = adapter->url;
  rpc_config.http_client = adapter->http_client;
  adapter->rpc_client = rpc_client_create(&rpc_config);
  if (!adapter->rpc_client) {
    flow_rpc_client_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  turbo_mutex_init(&adapter->lock);
  adapter->lock_initialized = 1;
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_rpc_client_start;
  ops.consume = flow_rpc_client_consume;
  ops.stop = flow_rpc_client_stop;
  ops.shutdown = flow_rpc_client_shutdown;
  ops.connection_snapshot = flow_rpc_client_connection_snapshot;
  rc = turbo_flow_register_adapter_ex(flow, name, &ops, adapter,
                                      config->poll_interval_ms > 0 ? &FLOW_RPC_POLL_SCHEMA
                                                                   : &FLOW_RPC_CLIENT_SCHEMA);
  if (rc != TURBO_OK) flow_rpc_client_shutdown(adapter);
  return rc;
}

int turbo_flow_rpc_register_client_resolved_adapter(turbo_flow_t *flow,
                                                    const turbo_flow_resolved_config_t *resolved,
                                                    const char *adapter_name) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  turbo_flow_rpc_client_config_t config;
  int have_method = 0;
  int have_url = 0;
  int rc;
  if (!flow) return TURBO_EINVAL;
  memset(&config, 0, sizeof(config));
  rc = turbo_flow_resolved_config_adapter(resolved, adapter_name, &view);
  if (rc != TURBO_OK) return rc;
  if (strcmp(view.kind, "rpc") != 0) return TURBO_EINVAL;
  for (size_t i = 0u; i < turbo_flow_resolved_adapter_field_count(&view); ++i) {
    const char *field = turbo_flow_resolved_adapter_field_name(&view, i);
    uint64_t number = 0u;
    if (!field) return TURBO_EPROTO;
    if (strcmp(field, "url") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &config.url);
      have_url = rc == TURBO_OK;
    } else if (strcmp(field, "method") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &config.method);
      have_method = rc == TURBO_OK;
    } else if (strcmp(field, "bearer_token") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &config.bearer_token);
    } else if (strcmp(field, "poll_params") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &config.poll_params);
    } else if (strcmp(field, "timeout_ms") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && number > INT_MAX) rc = TURBO_ERANGE;
      if (rc == TURBO_OK) config.timeout_ms = (int)number;
    } else if (strcmp(field, "poll_interval_ms") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && number > UINT32_MAX) rc = TURBO_ERANGE;
      if (rc == TURBO_OK) config.poll_interval_ms = (uint32_t)number;
    } else {
      rc = TURBO_EINVAL;
    }
    if (rc != TURBO_OK) return rc;
  }
  if (!have_url || !have_method) return TURBO_EINVAL;
  return turbo_flow_rpc_register_client_adapter(flow, adapter_name, &config);
}
