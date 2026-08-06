#include "turbo_flow_http_server.h"

#include "CoroNet.h"
#include "flow_connection.h"
#include "flow_http_content.h"
#include "iris/iris_app.h"
#include "iris/router.h"
#include "iris/server.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_HTTP_REQUEST_MAGIC UINT64_C(0x5446485454505251)
#define FLOW_HTTP_DEFAULT_RESPONSE_STATUS 200
#define FLOW_HTTP_DEFAULT_CONTENT_TYPE "application/octet-stream"

typedef struct flow_http_server_adapter_s flow_http_server_adapter_t;

typedef struct flow_http_request_context_s {
  uint64_t magic;
  flow_http_server_adapter_t *adapter;
  Res *response;
  int responded;
} flow_http_request_context_t;

struct flow_http_server_adapter_s {
  turbo_flow_t *flow;
  tstr_t name;
  tstr_t source_name;
  tstr_t route;
  tstr_t response_content_type;
  uint16_t port;
  size_t max_body_size;
  int response_status;
  coro_context_t *context;
  iris_app_t *app;
  coro_socket_t *server;
  int owns_context;
  int owns_app;
  atomic_int started;
  tf_connection_state_t connection;
  int loop_thread_started;
  turbo_thread_t loop_thread;
  flow_http_content_cache_t content_cache;
};

static const char *const FLOW_HTTP_METHOD_VALUES[] = {"get", "post", "put", "patch", "delete"};
static const turbo_flow_option_field_t FLOW_HTTP_SERVER_FIELDS[] = {
    {"port", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 65535,
     NULL, 0},
    {"route", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"method", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0, FLOW_HTTP_METHOD_VALUES,
     5},
    {"max_body_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"response_status", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 100, 599, NULL, 0},
    {"response_content_type", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"context", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"app", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"content_binding", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0,
     NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_HTTP_SERVER_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_HTTP,
    TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
    FLOW_HTTP_SERVER_FIELDS,
    sizeof(FLOW_HTTP_SERVER_FIELDS) / sizeof(FLOW_HTTP_SERVER_FIELDS[0])};

static int flow_http_server_method_valid(turbo_flow_http_method_t method) {
  return method >= TURBO_FLOW_HTTP_GET && method <= TURBO_FLOW_HTTP_DELETE;
}

static const char *flow_http_server_method_name(turbo_flow_http_method_t method) {
  switch (method) {
  case TURBO_FLOW_HTTP_GET:
    return "GET";
  case TURBO_FLOW_HTTP_POST:
    return "POST";
  case TURBO_FLOW_HTTP_PUT:
    return "PUT";
  case TURBO_FLOW_HTTP_PATCH:
    return "PATCH";
  case TURBO_FLOW_HTTP_DELETE:
    return "DELETE";
  default:
    return NULL;
  }
}

static int flow_http_server_has_unique_reply(const turbo_flow_t *flow, const char *source_name,
                                             const char *adapter_name) {
  size_t stage_count = turbo_flow_stage_count(flow);
  uint8_t *seen;
  uint32_t *queue;
  size_t head = 0;
  size_t tail = 0;
  int source_index = turbo_flow_find_stage(flow, source_name);
  int replies = 0;
  if (source_index < 0 || stage_count == 0) return 0;
  seen = (uint8_t *)calloc(stage_count, sizeof(*seen));
  queue = (uint32_t *)calloc(stage_count, sizeof(*queue));
  if (!seen || !queue) {
    free(seen);
    free(queue);
    return 0;
  }
  seen[source_index] = 1;
  queue[tail++] = (uint32_t)source_index;
  while (head < tail) {
    uint32_t current = queue[head++];
    for (size_t i = 0; i < turbo_flow_edge_count(flow); ++i) {
      const turbo_flow_edge_plan_t *edge = turbo_flow_edge_at(flow, i);
      if (!edge || edge->from_stage != current || edge->to_stage >= stage_count ||
          seen[edge->to_stage])
        continue;
      seen[edge->to_stage] = 1;
      queue[tail++] = edge->to_stage;
    }
  }
  for (size_t i = 0; i < stage_count; ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    if (seen[i] && stage && !stage->is_source && stage->adapter_name &&
        strcmp(stage->adapter_name, adapter_name) == 0)
      replies += 1;
  }
  free(seen);
  free(queue);
  return replies == 1;
}

static void flow_http_server_handler(Req *req, Res *res) {
  flow_http_server_adapter_t *adapter;
  flow_http_request_context_t request_context;
  turbo_flow_msg_t msg;
  int rc;
  if (!req || !res || !req->app || !req->path) return;
  adapter = (flow_http_server_adapter_t *)iris_app_lookup_rpc_context(req->app, req->path);
  if (!adapter || !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    send_text(res, 503, "service unavailable");
    return;
  }
  if (adapter->max_body_size > 0 && req->body_len > adapter->max_body_size) {
    send_text(res, 413, "payload too large");
    return;
  }
  if (tf_connection_request_begin(&adapter->connection, req->body_len) != TURBO_OK) {
    send_text(res, 500, "request accounting failed");
    return;
  }
  memset(&request_context, 0, sizeof(request_context));
  request_context.magic = FLOW_HTTP_REQUEST_MAGIC;
  request_context.adapter = adapter;
  request_context.response = res;
  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_new_len(req->body ? req->body : "", req->body_len);
  if (!msg.owned_payload) {
    atomic_store_explicit(&adapter->connection.last_status, TURBO_ENOMEM, memory_order_relaxed);
    (void)tf_connection_request_end(&adapter->connection, req->body_len);
    send_text(res, 500, "out of memory");
    return;
  }
  msg.payload = tstr_to_v(msg.owned_payload);
  msg.transport_context = &request_context;
  {
    const char *content_type = get_headers(req, "Content-Type");
    const turbo_flow_content_descriptor_t *descriptor = NULL;
    if (content_type || req->body_len > 0u) {
      rc = flow_http_content_cache_get(&adapter->content_cache,
                                       content_type ? content_type : FLOW_HTTP_DEFAULT_CONTENT_TYPE,
                                       &descriptor);
      if (rc != TURBO_OK && rc != TURBO_ENOENT) {
        turbo_flow_msg_cleanup(&msg);
        atomic_store_explicit(&adapter->connection.last_status, rc, memory_order_relaxed);
        (void)tf_connection_request_end(&adapter->connection, req->body_len);
        send_text(res, 400, "content schema conflict");
        return;
      }
      if (descriptor) {
        rc = turbo_flow_msg_set_content_descriptor(&msg, descriptor);
        if (rc != TURBO_OK) {
          turbo_flow_msg_cleanup(&msg);
          atomic_store_explicit(&adapter->connection.last_status, rc, memory_order_relaxed);
          (void)tf_connection_request_end(&adapter->connection, req->body_len);
          send_text(res, 500, "content descriptor failed");
          return;
        }
      }
    }
  }
  rc = turbo_flow_publish(adapter->flow, adapter->source_name, &msg);
  turbo_flow_msg_cleanup(&msg);
  atomic_store_explicit(&adapter->connection.last_status, rc, memory_order_relaxed);
  (void)tf_connection_request_end(&adapter->connection, req->body_len);
  if (rc != TURBO_OK && !request_context.responded) {
    send_text(res, 500, "flow processing failed");
  } else if (!request_context.responded) {
    send_text(res, 500, "flow reply stage was not reached");
  }
}

static void flow_http_server_context_thread(void *arg) {
  flow_http_server_adapter_t *adapter = (flow_http_server_adapter_t *)arg;
  if (!adapter || !adapter->context) return;
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    turbo_sleep_ms(1);
  }
  if (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    (void)coro_context_run(adapter->context, TURBO_RUN_DEFAULT);
  }
}

static int flow_http_server_start(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage) {
  flow_http_server_adapter_t *adapter = (flow_http_server_adapter_t *)ctx;
  if (!adapter || !stage) return TURBO_EINVAL;
  if (!stage->is_source) return TURBO_OK;
  if (!flow_http_server_has_unique_reply(flow, stage->name, adapter->name)) {
    return TURBO_EINVAL;
  }
  tstr_freep(&adapter->source_name);
  adapter->source_name = tstr_dup(stage->name);
  if (!adapter->source_name) return TURBO_ENOMEM;
  adapter->flow = flow;
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_OK);
  adapter->server = iris_server_start(adapter->app, adapter->context, adapter->port);
  if (!adapter->server) {
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_EIO);
    return TURBO_EIO;
  }
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  tf_connection_set_usage(&adapter->connection, 1u, 0u, 0u);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
  if (adapter->owns_context && !adapter->loop_thread_started) {
    if (turbo_thread_create(&adapter->loop_thread, flow_http_server_context_thread, adapter) !=
        TURBO_OK) {
      atomic_store_explicit(&adapter->started, 0, memory_order_release);
      coro_socket_destroy(adapter->server);
      adapter->server = NULL;
      tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
      tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_EINVAL);
      return TURBO_EINVAL;
    }
    adapter->loop_thread_started = 1;
  }
  return TURBO_OK;
}

static int flow_http_server_consume(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_http_server_adapter_t *adapter = (flow_http_server_adapter_t *)ctx;
  flow_http_request_context_t *request;
  (void)flow;
  (void)stage;
  if (!adapter || !msg || !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    return TURBO_EINVAL;
  }
  request = (flow_http_request_context_t *)msg->transport_context;
  if (!request || request->magic != FLOW_HTTP_REQUEST_MAGIC || request->adapter != adapter ||
      !request->response || request->responded) {
    return TURBO_EINVAL;
  }
  reply(request->response, adapter->response_status,
        adapter->response_content_type ? adapter->response_content_type
                                       : FLOW_HTTP_DEFAULT_CONTENT_TYPE,
        msg->payload.data ? msg->payload.data : "", msg->payload.len);
  request->responded = 1;
  return TURBO_OK;
}

static void flow_http_server_stop(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage) {
  flow_http_server_adapter_t *adapter = (flow_http_server_adapter_t *)ctx;
  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_OK);
  if (adapter->server) {
    coro_socket_destroy(adapter->server);
    adapter->server = NULL;
  }
  if (adapter->loop_thread_started) {
    coro_context_stop(adapter->context);
    (void)turbo_thread_join(&adapter->loop_thread);
    adapter->loop_thread_started = 0;
  }
  tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_STOPPED, TURBO_ESHUTDOWN);
}

static int flow_http_server_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flow_http_server_adapter_t *adapter = (flow_http_server_adapter_t *)ctx;
  return adapter ? tf_connection_snapshot(&adapter->connection, out) : TURBO_EINVAL;
}

static void flow_http_server_shutdown(void *ctx) {
  flow_http_server_adapter_t *adapter = (flow_http_server_adapter_t *)ctx;
  if (!adapter) return;
  flow_http_server_stop(adapter, NULL, NULL);
  if (adapter->app && adapter->route) {
    (void)iris_app_unbind_rpc_context(adapter->app, adapter->route, adapter);
  }
  if (adapter->owns_app && adapter->app) iris_app_destroy(adapter->app);
  if (adapter->owns_context && adapter->context) {
    coro_context_destroy(adapter->context);
  }
  tstr_freep(&adapter->name);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->route);
  tstr_freep(&adapter->response_content_type);
  flow_http_content_cache_destroy(&adapter->content_cache);
  free(adapter);
}

int turbo_flow_http_register_server_adapter(turbo_flow_t *flow, const char *name,
                                            const turbo_flow_http_server_config_t *config) {
  static const char *const operations[] = {TURBO_FLOW_HTTP_SERVER_REQUEST_OPERATION,
                                           TURBO_FLOW_HTTP_SERVER_REPLY_OPERATION};
  flow_http_server_adapter_t *adapter;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_module_adapter_registration_t registration =
      TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
  turbo_flow_primitive_descriptor_t primitive;
  const char *operation_resources[2] = {name, name};
  int rc;
  if (!flow || !name || name[0] == '\0' || !config || config->port == 0 || !config->route ||
      config->route[0] != '/' || !flow_http_server_method_valid(config->method) ||
      config->response_status < 0 || config->response_status > 599) {
    return TURBO_EINVAL;
  }
  rc = flow_http_register_server_module_contract(flow);
  if (rc != TURBO_OK) return rc;
  adapter = (flow_http_server_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  {
    char endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
    int written = snprintf(endpoint, sizeof(endpoint), "http://0.0.0.0:%u%s",
                           (unsigned int)config->port, config->route);
    if (written < 0 || (size_t)written >= sizeof(endpoint) ||
        tf_connection_init(&adapter->connection, endpoint, 1u) != TURBO_OK) {
      free(adapter);
      return TURBO_ENOSPC;
    }
  }
  adapter->port = config->port;
  adapter->max_body_size = config->max_body_size;
  adapter->response_status =
      config->response_status ? config->response_status : FLOW_HTTP_DEFAULT_RESPONSE_STATUS;
  adapter->name = tstr_dup(name);
  adapter->route = tstr_dup(config->route);
  adapter->response_content_type =
      tstr_dup(config->response_content_type ? config->response_content_type
                                             : FLOW_HTTP_DEFAULT_CONTENT_TYPE);
  if (!adapter->name || !adapter->route || !adapter->response_content_type) {
    flow_http_server_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  rc = flow_http_content_cache_init(&adapter->content_cache,
                                    TURBO_FLOW_CONTENT_PROFILE_HTTP_REQUEST_BODY, adapter->route,
                                    config->content_binding);
  if (rc != TURBO_OK) {
    flow_http_server_shutdown(adapter);
    return rc;
  }
  adapter->context = config->context ? config->context : coro_context_create(NULL);
  adapter->owns_context = config->context ? config->take_context_ownership != 0 : 1;
  adapter->app = config->app ? config->app : iris_app_create();
  adapter->owns_app = config->app ? config->take_app_ownership != 0 : 1;
  if (!adapter->context || !adapter->app) {
    flow_http_server_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  if (iris_app_bind_rpc_context(adapter->app, adapter->route, adapter) != 0) {
    flow_http_server_shutdown(adapter);
    return TURBO_EALREADY;
  }
  iris_app_route(adapter->app, flow_http_server_method_name(config->method), adapter->route, NO_MW,
                 flow_http_server_handler);
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_http_server_start;
  ops.consume = flow_http_server_consume;
  ops.stop = flow_http_server_stop;
  ops.shutdown = flow_http_server_shutdown;
  ops.connection_snapshot = flow_http_server_connection_snapshot;
  memset(&primitive, 0, sizeof(primitive));
  primitive.size = sizeof(primitive);
  primitive.name = name;
  primitive.type_name = TURBO_FLOW_HTTP_SERVER_PRIMITIVE_TYPE;
  primitive.version = TURBO_FLOW_HTTP_MODULE_VERSION;
  primitive.domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
  primitive.kind = TURBO_FLOW_PRIMITIVE_RESOURCE;
  registration.module_name = TURBO_FLOW_HTTP_SERVER_MODULE;
  registration.adapter_name = name;
  registration.ops = &ops;
  registration.ctx = adapter;
  registration.schema = &FLOW_HTTP_SERVER_SCHEMA;
  registration.operation_names = operations;
  registration.operation_count = sizeof(operations) / sizeof(operations[0]);
  registration.operation_resource_names = operation_resources;
  registration.primitives = &primitive;
  registration.primitive_count = 1u;
  rc = turbo_flow_register_module_adapter(flow, &registration);
  if (rc != TURBO_OK) flow_http_server_shutdown(adapter);
  return rc;
}

int turbo_flow_http_register_server_resolved_adapter(turbo_flow_t *flow,
                                                     const turbo_flow_resolved_config_t *resolved,
                                                     const char *adapter_name) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  turbo_flow_http_server_config_t config;
  int have_method = 0;
  int have_port = 0;
  int have_route = 0;
  int rc;
  if (!flow) return TURBO_EINVAL;
  memset(&config, 0, sizeof(config));
  rc = turbo_flow_resolved_config_adapter(resolved, adapter_name, &view);
  if (rc != TURBO_OK) return rc;
  if (strcmp(view.kind, "http") != 0) return TURBO_EINVAL;
  for (size_t i = 0u; i < turbo_flow_resolved_adapter_field_count(&view); ++i) {
    const char *field = turbo_flow_resolved_adapter_field_name(&view, i);
    uint64_t number = 0u;
    if (!field) return TURBO_EPROTO;
    if (strcmp(field, "port") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && (number == 0u || number > UINT16_MAX)) rc = TURBO_ERANGE;
      if (rc == TURBO_OK) config.port = (uint16_t)number;
      have_port = rc == TURBO_OK;
    } else if (strcmp(field, "route") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &config.route);
      have_route = rc == TURBO_OK;
    } else if (strcmp(field, "method") == 0) {
      const char *method = NULL;
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &method);
      if (rc == TURBO_OK) rc = TURBO_EINVAL;
      for (size_t method_index = 0u;
           method &&
           method_index < sizeof(FLOW_HTTP_METHOD_VALUES) / sizeof(FLOW_HTTP_METHOD_VALUES[0]);
           ++method_index) {
        if (strcmp(method, FLOW_HTTP_METHOD_VALUES[method_index]) == 0) {
          config.method = (turbo_flow_http_method_t)method_index;
          rc = TURBO_OK;
          break;
        }
      }
      have_method = rc == TURBO_OK;
    } else if (strcmp(field, "max_body_size") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && number > SIZE_MAX) rc = TURBO_ERANGE;
      if (rc == TURBO_OK) config.max_body_size = (size_t)number;
    } else if (strcmp(field, "response_status") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && (number < 100u || number > 599u)) rc = TURBO_ERANGE;
      if (rc == TURBO_OK) config.response_status = (int)number;
    } else if (strcmp(field, "response_content_type") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &config.response_content_type);
    } else {
      rc = TURBO_EINVAL;
    }
    if (rc != TURBO_OK) return rc;
  }
  if (!have_port || !have_route || !have_method) return TURBO_EINVAL;
  return turbo_flow_http_register_server_adapter(flow, adapter_name, &config);
}
