#include "turbo_flow_rpc.h"

#include "CoroNet.h"
#include "flow_connection.h"
#include "iris/iris_app.h"
#include "iris/router.h"
#include "iris/rpc_server.h"
#include "iris/server.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_RPC_REQUEST_MAGIC UINT64_C(0x5446525043524551)

typedef struct flow_rpc_server_s flow_rpc_server_t;

typedef struct flow_rpc_request_context_s {
  uint64_t magic;
  flow_rpc_server_t *server;
  Res *http_response;
  rpc_response_t rpc_response;
  const char *method;
  int notification;
  int responded;
} flow_rpc_request_context_t;

struct flow_rpc_server_s {
  turbo_flow_t *flow;
  tstr name;
  tstr source_name;
  tstr endpoint;
  uint16_t port;
  size_t max_request_size;
  coro_context_t *context;
  iris_app_t *app;
  coro_socket_t *socket;
  int owns_context;
  int owns_app;
  atomic_int started;
  tf_connection_state_t connection;
  int loop_thread_started;
  turbo_thread_t loop_thread;
};

static int flow_rpc_register_server_contract(turbo_flow_t *flow) {
  static const char *const primitive_types[] = {TURBO_FLOW_RPC_SERVER_PRIMITIVE_TYPE};
  static const char *const operation_names[] = {TURBO_FLOW_RPC_SERVER_REQUEST_OPERATION,
                                                TURBO_FLOW_RPC_SERVER_REPLY_OPERATION};
  turbo_flow_operation_descriptor_t operations[2];
  turbo_flow_module_descriptor_t module;
  memset(operations, 0, sizeof(operations));
  memset(&module, 0, sizeof(module));
  for (size_t i = 0; i < 2u; ++i) {
    operations[i].size = sizeof(operations[i]);
    operations[i].name = operation_names[i];
    operations[i].version = TURBO_FLOW_RPC_MODULE_VERSION;
    operations[i].domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
    operations[i].resource_domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
    operations[i].resource_type = TURBO_FLOW_RPC_SERVER_PRIMITIVE_TYPE;
    operations[i].resource_min_version = TURBO_FLOW_RPC_MODULE_VERSION;
    operations[i].resource_max_version = TURBO_FLOW_RPC_MODULE_VERSION;
    operations[i].scope.data = TURBO_FLOW_DATA_SCOPE_MESSAGE;
    operations[i].scope.state = TURBO_FLOW_STATE_SCOPE_RESOURCE_OWNER;
    operations[i].scope.lifetime = i == 0u ? TURBO_FLOW_LIFETIME_DISPATCH
                                            : TURBO_FLOW_LIFETIME_CALL;
    operations[i].scope.concurrency = TURBO_FLOW_CONCURRENCY_OWNER_CONTEXT;
    operations[i].scope.authority = TURBO_FLOW_AUTHORITY_OWNER_LOCAL;
    operations[i].flags = (i == 0u ? TURBO_FLOW_OPERATION_SOURCE
                                   : TURBO_FLOW_OPERATION_STAGE) |
                          TURBO_FLOW_OPERATION_BRIDGE;
    operations[i].execution_mask = TURBO_FLOW_OPERATION_EXEC_INLINE;
  }
  operations[0].output_domain = TURBO_FLOW_DOMAIN_DATA;
  operations[0].output_type = "Message";
  operations[1].input_domain = TURBO_FLOW_DOMAIN_DATA;
  operations[1].input_type = "Message";
  module.size = sizeof(module);
  module.name = TURBO_FLOW_RPC_SERVER_MODULE;
  module.version = TURBO_FLOW_RPC_MODULE_VERSION;
  module.capability_flags = TURBO_FLOW_MODULE_GRAPH_OPERATIONS |
                            TURBO_FLOW_MODULE_MANAGED_RESOURCES |
                            TURBO_FLOW_MODULE_NATIVE_API;
  module.primitive_types = primitive_types;
  module.primitive_type_count = 1u;
  module.operation_names = operation_names;
  module.operation_count = 2u;
  return turbo_flow_register_module_contract(flow, &module, operations, 2u);
}

static const turbo_flow_option_field_t FLOW_RPC_SERVER_FIELDS[] = {
    {"port", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 65535,
     NULL, 0},
    {"endpoint", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"max_request_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"context", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"app", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_RPC_SERVER_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_RPC,
    TURBO_FLOW_ADAPTER_SOURCE | TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
    FLOW_RPC_SERVER_FIELDS,
    sizeof(FLOW_RPC_SERVER_FIELDS) / sizeof(FLOW_RPC_SERVER_FIELDS[0])};

static int flow_rpc_has_unique_reply(const turbo_flow_t *flow, const char *source_name,
                                     const char *adapter_name) {
  size_t count = turbo_flow_stage_count(flow);
  uint8_t *seen;
  uint32_t *queue;
  size_t head = 0;
  size_t tail = 0;
  int source = turbo_flow_find_stage(flow, source_name);
  int replies = 0;

  if (source < 0 || count == 0) return 0;
  seen = (uint8_t *)calloc(count, sizeof(*seen));
  queue = (uint32_t *)calloc(count, sizeof(*queue));
  if (!seen || !queue) {
    free(seen);
    free(queue);
    return 0;
  }
  seen[source] = 1;
  queue[tail++] = (uint32_t)source;
  while (head < tail) {
    uint32_t current = queue[head++];
    for (size_t i = 0; i < turbo_flow_edge_count(flow); ++i) {
      const turbo_flow_edge_plan_t *edge = turbo_flow_edge_at(flow, i);
      if (edge && edge->from_stage == current && edge->to_stage < count && !seen[edge->to_stage]) {
        seen[edge->to_stage] = 1;
        queue[tail++] = edge->to_stage;
      }
    }
  }
  for (size_t i = 0; i < count; ++i) {
    const turbo_flow_stage_plan_t *stage = turbo_flow_stage_at(flow, i);
    if (seen[i] && stage && !stage->is_source && stage->adapter_name &&
        strcmp(stage->adapter_name, adapter_name) == 0) {
      replies += 1;
    }
  }
  free(seen);
  free(queue);
  return replies == 1;
}

static void flow_rpc_server_handler(Req *req, Res *res) {
  flow_rpc_server_t *server;
  flow_rpc_request_context_t request_context;
  rpc_request_t rpc_request;
  turbo_flow_msg_t msg;
  const char *params;
  int rc;

  if (!req || !res || !req->app || !req->path) return;
  server = (flow_rpc_server_t *)iris_app_lookup_rpc_context(req->app, req->path);
  if (!server || !atomic_load_explicit(&server->started, memory_order_acquire)) {
    rpc_send_error(res, RPC_ERROR_INTERNAL, "service unavailable", NULL);
    return;
  }
  if (server->max_request_size > 0 && req->body_len > server->max_request_size) {
    rpc_send_error(res, RPC_ERROR_INVALID_REQUEST, "request too large", NULL);
    return;
  }
  if (tf_connection_request_begin(&server->connection, req->body_len) != TURBO_OK) {
    rpc_send_error(res, RPC_ERROR_INTERNAL, "request accounting failed", NULL);
    return;
  }
  memset(&rpc_request, 0, sizeof(rpc_request));
  rc = rpc_parse_request(req, &rpc_request);
  if (rc != 0) {
    atomic_store_explicit(&server->connection.last_status, TURBO_EPROTO, memory_order_relaxed);
    (void)tf_connection_request_end(&server->connection, req->body_len);
    rpc_send_error(res, rc, "invalid RPC request", NULL);
    return;
  }

  memset(&request_context, 0, sizeof(request_context));
  request_context.magic = FLOW_RPC_REQUEST_MAGIC;
  request_context.server = server;
  request_context.http_response = res;
  request_context.method = rpc_request.method;
  request_context.notification = rpc_request.id == NULL;
  request_context.rpc_response.arena = req->arena;
  request_context.rpc_response.jsonrpc = rpc_request.jsonrpc;
  request_context.rpc_response.id = rpc_request.id;
  request_context.rpc_response.protocol = rpc_request.protocol;
  params = rpc_request.params ? rpc_request.params : "null";

  turbo_flow_msg_init(&msg);
  msg.owned_payload = tstr_dup(params);
  if (!msg.owned_payload) {
    atomic_store_explicit(&server->connection.last_status, TURBO_ENOMEM, memory_order_relaxed);
    (void)tf_connection_request_end(&server->connection, req->body_len);
    rpc_send_error(res, RPC_ERROR_INTERNAL, "out of memory", rpc_request.id);
    return;
  }
  msg.payload = tstr_to_v(msg.owned_payload);
  msg.transport_context = &request_context;
  rc = turbo_flow_publish(server->flow, server->source_name, &msg);
  turbo_flow_msg_cleanup(&msg);
  atomic_store_explicit(&server->connection.last_status, rc, memory_order_relaxed);
  (void)tf_connection_request_end(&server->connection, req->body_len);
  if (rc != TURBO_OK && !request_context.responded) {
    rpc_send_error(res, RPC_ERROR_INTERNAL, "flow processing failed", rpc_request.id);
  } else if (!request_context.responded) {
    rpc_send_error(res, RPC_ERROR_INTERNAL, "flow reply stage was not reached", rpc_request.id);
  } else if (request_context.notification) {
    reply(res, 204, "application/json", "", 0);
  }
}

static void flow_rpc_loop_thread(void *arg) {
  flow_rpc_server_t *server = (flow_rpc_server_t *)arg;
  if (!server || !server->context) return;
  while (atomic_load_explicit(&server->started, memory_order_acquire) && server->flow &&
         turbo_flow_state(server->flow) != TURBO_FLOW_STATE_STARTED) {
    turbo_sleep_ms(1);
  }
  if (atomic_load_explicit(&server->started, memory_order_acquire)) {
    (void)coro_context_run(server->context, TURBO_RUN_DEFAULT);
  }
}

static int flow_rpc_server_start(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flow_rpc_server_t *server = (flow_rpc_server_t *)ctx;
  if (!server || !stage) return TURBO_EINVAL;
  if (!stage->is_source) return TURBO_OK;
  if (!flow_rpc_has_unique_reply(flow, stage->name, server->name)) return TURBO_EINVAL;
  tstr_freep(&server->source_name);
  server->source_name = tstr_dup(stage->name);
  if (!server->source_name) return TURBO_ENOMEM;
  server->flow = flow;
  tf_connection_transition(&server->connection, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_OK);
  server->socket = iris_server_start(server->app, server->context, server->port);
  if (!server->socket) {
    tf_connection_transition(&server->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_EIO);
    return TURBO_EIO;
  }
  atomic_store_explicit(&server->started, 1, memory_order_release);
  tf_connection_set_usage(&server->connection, 1u, 0u, 0u);
  tf_connection_transition(&server->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
  if (server->owns_context && !server->loop_thread_started) {
    if (turbo_thread_create(&server->loop_thread, flow_rpc_loop_thread, server) != TURBO_OK) {
      atomic_store_explicit(&server->started, 0, memory_order_release);
      coro_socket_destroy(server->socket);
      server->socket = NULL;
      tf_connection_set_usage(&server->connection, 0u, 0u, 0u);
      tf_connection_transition(&server->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_EINVAL);
      return TURBO_EINVAL;
    }
    server->loop_thread_started = 1;
  }
  return TURBO_OK;
}

static int flow_rpc_server_consume(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_rpc_server_t *server = (flow_rpc_server_t *)ctx;
  flow_rpc_request_context_t *request;
  char *result;
  (void)flow;
  (void)stage;
  if (!server || !msg || !atomic_load_explicit(&server->started, memory_order_acquire)) {
    return TURBO_EINVAL;
  }
  request = (flow_rpc_request_context_t *)msg->transport_context;
  if (!request || request->magic != FLOW_RPC_REQUEST_MAGIC || request->server != server ||
      request->responded) {
    return TURBO_EINVAL;
  }
  if (!request->notification) {
    result = (char *)malloc(msg->payload.len + 1);
    if (!result) return TURBO_ENOMEM;
    if (msg->payload.len > 0) memcpy(result, msg->payload.data, msg->payload.len);
    result[msg->payload.len] = '\0';
    rpc_set_result(&request->rpc_response, result);
    free(result);
    rpc_send_response(request->http_response, &request->rpc_response);
  }
  request->responded = 1;
  return TURBO_OK;
}

static void flow_rpc_server_stop(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flow_rpc_server_t *server = (flow_rpc_server_t *)ctx;
  (void)flow;
  (void)stage;
  if (!server) return;
  atomic_store_explicit(&server->started, 0, memory_order_release);
  tf_connection_transition(&server->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_OK);
  if (server->socket) {
    coro_socket_destroy(server->socket);
    server->socket = NULL;
  }
  if (server->loop_thread_started) {
    coro_context_stop(server->context);
    (void)turbo_thread_join(&server->loop_thread);
    server->loop_thread_started = 0;
  }
  tf_connection_set_usage(&server->connection, 0u, 0u, 0u);
  tf_connection_transition(&server->connection, TURBO_FLOW_CONNECTION_STOPPED, TURBO_ESHUTDOWN);
}

static int flow_rpc_server_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flow_rpc_server_t *server = (flow_rpc_server_t *)ctx;
  return server ? tf_connection_snapshot(&server->connection, out) : TURBO_EINVAL;
}

static void flow_rpc_server_shutdown(void *ctx) {
  flow_rpc_server_t *server = (flow_rpc_server_t *)ctx;
  if (!server) return;
  flow_rpc_server_stop(server, NULL, NULL);
  if (server->app && server->endpoint) {
    (void)iris_app_unbind_rpc_context(server->app, server->endpoint, server);
  }
  if (server->owns_app && server->app) iris_app_destroy(server->app);
  if (server->owns_context && server->context) coro_context_destroy(server->context);
  tstr_freep(&server->name);
  tstr_freep(&server->source_name);
  tstr_freep(&server->endpoint);
  free(server);
}

int turbo_flow_rpc_register_server_adapter(turbo_flow_t *flow, const char *name,
                                           const turbo_flow_rpc_server_config_t *config) {
  static const char *const operations[] = {TURBO_FLOW_RPC_SERVER_REQUEST_OPERATION,
                                           TURBO_FLOW_RPC_SERVER_REPLY_OPERATION};
  flow_rpc_server_t *server;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_module_adapter_registration_t registration =
      TURBO_FLOW_MODULE_ADAPTER_REGISTRATION_INIT;
  turbo_flow_primitive_descriptor_t primitive;
  const char *operation_resources[2] = {name, name};
  int rc;
  if (!flow || !name || name[0] == '\0' || !config || config->port == 0 || !config->endpoint ||
      config->endpoint[0] != '/') {
    return TURBO_EINVAL;
  }
  rc = flow_rpc_register_server_contract(flow);
  if (rc != TURBO_OK) return rc;
  server = (flow_rpc_server_t *)calloc(1, sizeof(*server));
  if (!server) return TURBO_ENOMEM;
  atomic_init(&server->started, 0);
  {
    char connection_endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
    int written = snprintf(connection_endpoint, sizeof(connection_endpoint), "rpc://0.0.0.0:%u%s",
                           (unsigned int)config->port, config->endpoint);
    if (written < 0 || (size_t)written >= sizeof(connection_endpoint) ||
        tf_connection_init(&server->connection, connection_endpoint, 1u) != TURBO_OK) {
      free(server);
      return TURBO_ENOSPC;
    }
  }
  server->port = config->port;
  server->max_request_size = config->max_request_size;
  server->name = tstr_dup(name);
  server->endpoint = tstr_dup(config->endpoint);
  server->context = config->context ? config->context : coro_context_create(NULL);
  server->owns_context = config->context ? (config->take_context_ownership != 0) : 1;
  server->app = config->app ? config->app : iris_app_create();
  server->owns_app = config->app ? (config->take_app_ownership != 0) : 1;
  if (!server->name || !server->endpoint || !server->context || !server->app) {
    flow_rpc_server_shutdown(server);
    return TURBO_ENOMEM;
  }
  if (iris_app_bind_rpc_context(server->app, server->endpoint, server) != 0) {
    flow_rpc_server_shutdown(server);
    return TURBO_EALREADY;
  }
  iris_app_post(server->app, server->endpoint, flow_rpc_server_handler);
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_rpc_server_start;
  ops.consume = flow_rpc_server_consume;
  ops.stop = flow_rpc_server_stop;
  ops.shutdown = flow_rpc_server_shutdown;
  ops.connection_snapshot = flow_rpc_server_connection_snapshot;
  memset(&primitive, 0, sizeof(primitive));
  primitive.size = sizeof(primitive);
  primitive.name = name;
  primitive.type_name = TURBO_FLOW_RPC_SERVER_PRIMITIVE_TYPE;
  primitive.version = TURBO_FLOW_RPC_MODULE_VERSION;
  primitive.domain = TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
  primitive.kind = TURBO_FLOW_PRIMITIVE_RESOURCE;
  registration.module_name = TURBO_FLOW_RPC_SERVER_MODULE;
  registration.adapter_name = name;
  registration.ops = &ops;
  registration.ctx = server;
  registration.schema = &FLOW_RPC_SERVER_SCHEMA;
  registration.operation_names = operations;
  registration.operation_count = 2u;
  registration.operation_resource_names = operation_resources;
  registration.primitives = &primitive;
  registration.primitive_count = 1u;
  rc = turbo_flow_register_module_adapter(flow, &registration);
  if (rc != TURBO_OK) flow_rpc_server_shutdown(server);
  return rc;
}

int turbo_flow_rpc_register_server_resolved_adapter(turbo_flow_t *flow,
                                                    const turbo_flow_resolved_config_t *resolved,
                                                    const char *adapter_name) {
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  turbo_flow_rpc_server_config_t config;
  int have_endpoint = 0;
  int have_port = 0;
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
    if (strcmp(field, "port") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && (number == 0u || number > UINT16_MAX)) rc = TURBO_ERANGE;
      if (rc == TURBO_OK) config.port = (uint16_t)number;
      have_port = rc == TURBO_OK;
    } else if (strcmp(field, "endpoint") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &config.endpoint);
      have_endpoint = rc == TURBO_OK;
    } else if (strcmp(field, "max_request_size") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && number > SIZE_MAX) rc = TURBO_ERANGE;
      if (rc == TURBO_OK) config.max_request_size = (size_t)number;
    } else {
      rc = TURBO_EINVAL;
    }
    if (rc != TURBO_OK) return rc;
  }
  if (!have_port || !have_endpoint) return TURBO_EINVAL;
  return turbo_flow_rpc_register_server_adapter(flow, adapter_name, &config);
}

const char *turbo_flow_rpc_request_method(const turbo_flow_msg_t *msg) {
  const flow_rpc_request_context_t *request;
  if (!msg) return NULL;
  request = (const flow_rpc_request_context_t *)msg->transport_context;
  if (!request || request->magic != FLOW_RPC_REQUEST_MAGIC) return NULL;
  return request->method;
}
