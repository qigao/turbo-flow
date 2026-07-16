#include "turbo_flow_http_client.h"

#include "CoroNet.h"
#include "flow_connection.h"
#include "flow_http_content.h"
#include "flow_resource_status.h"
#include "flow_timer.h"
#include "fmt.h"
#include "http/http_client.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_HTTP_DEFAULT_STATUS_MIN 200
#define FLOW_HTTP_DEFAULT_STATUS_MAX 299
#define FLOW_HTTP_DEFAULT_PUMP_ITERATIONS 10000u
#define FLOW_HTTP_RESOLVED_HEADER_MAX 64u

typedef struct flow_http_client_adapter_s flow_http_client_adapter_t;

typedef struct flow_http_client_task_s {
  flow_http_client_adapter_t *adapter;
  const char *body;
  size_t body_len;
  http_response_t *response;
  int done;
} flow_http_client_task_t;

struct flow_http_client_adapter_s {
  turbo_flow_t *flow;
  tstr_t source_name;
  tstr_t resource_uid;
  tstr_t resource_owner;
  tstr_t url;
  tstr_t bearer_token;
  char **headers;
  size_t header_count;
  turbo_flow_http_method_t method;
  int success_status_min;
  int success_status_max;
  uint32_t max_pump_iterations;
  uint32_t poll_interval_ms;
  http_client_t *client;
  int owns_client;
  atomic_int started;
  atomic_int quiesced;
  tf_connection_state_t connection;
  tf_timer_t wait_timer;
  int wait_timer_initialized;
  turbo_mutex_t lock;
  int lock_initialized;
  int loop_thread_started;
  turbo_thread_t loop_thread;
  flow_http_content_cache_t content_cache;
};

static const char *const FLOW_HTTP_METHOD_VALUES[] = {"get", "post", "put", "patch", "delete"};
static const turbo_flow_option_field_t FLOW_HTTP_CLIENT_FIELDS[] = {
    {"client", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"url", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"method", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0, FLOW_HTTP_METHOD_VALUES,
     5},
    {"headers", TURBO_FLOW_OPTION_STRING_LIST, 0, 0, 0, NULL, 0},
    {"bearer_token", TURBO_FLOW_OPTION_SECRET, TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"max_response_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"success_status_min", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 100, 599, NULL, 0},
    {"success_status_max", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 100, 599, NULL, 0},
    {"max_pump_iterations", TURBO_FLOW_OPTION_U32, 0, 0, 0, NULL, 0},
    {"poll_interval_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"content_binding", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0,
     NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_HTTP_CLIENT_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_HTTP,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_BIDIRECTIONAL,
    FLOW_HTTP_CLIENT_FIELDS,
    sizeof(FLOW_HTTP_CLIENT_FIELDS) / sizeof(FLOW_HTTP_CLIENT_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_HTTP_POLL_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_HTTP,
    TURBO_FLOW_ADAPTER_SOURCE,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_HTTP_CLIENT_FIELDS,
    sizeof(FLOW_HTTP_CLIENT_FIELDS) / sizeof(FLOW_HTTP_CLIENT_FIELDS[0])};

static const char FLOW_HTTP_RESOURCE_SCHEMA_TEXT[] =
    "schema TurboFlowHttpResource [id(101), version(1)];\n"
    "message HttpClientStatus {\n"
    "  uint32 state;\n"
    "  int32 last_status;\n"
    "  string connections_current;\n"
    "  string connection_limit;\n"
    "  string in_flight_messages;\n"
    "  string in_flight_bytes;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_HTTP_RESOURCE_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_IO_TRANSPORT,
    TURBO_FLOW_RESOURCE_CONNECTION,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowHttpResource",
    "HttpClientStatus",
    101u,
    1u,
    FLOW_HTTP_RESOURCE_SCHEMA_TEXT};

static int flow_http_client_method_valid(turbo_flow_http_method_t method) {
  return method >= TURBO_FLOW_HTTP_GET && method <= TURBO_FLOW_HTTP_DELETE;
}

static int flow_http_client_method_value(turbo_flow_http_method_t method) {
  switch (method) {
  case TURBO_FLOW_HTTP_GET:
    return HTTP_GET;
  case TURBO_FLOW_HTTP_POST:
    return HTTP_POST;
  case TURBO_FLOW_HTTP_PUT:
    return HTTP_PUT;
  case TURBO_FLOW_HTTP_PATCH:
    return HTTP_PATCH;
  case TURBO_FLOW_HTTP_DELETE:
    return HTTP_DELETE;
  default:
    return -1;
  }
}

static int flow_http_client_replace_payload(turbo_flow_msg_t *msg, const char *data, size_t len) {
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

static char *flow_http_client_strdup(const char *value) {
  size_t len;
  char *copy;
  if (!value) return NULL;
  len = strlen(value);
  copy = (char *)malloc(len + 1);
  if (copy) memcpy(copy, value, len + 1);
  return copy;
}

static int flow_http_client_copy_headers(flow_http_client_adapter_t *adapter,
                                         const char *const *headers, size_t count) {
  if (count == 0) return TURBO_OK;
  if (!headers || count > (size_t)INT32_MAX) return TURBO_EINVAL;
  adapter->headers = (char **)calloc(count, sizeof(char *));
  if (!adapter->headers) return TURBO_ENOMEM;
  adapter->header_count = count;
  for (size_t i = 0; i < count; ++i) {
    if (!headers[i] || headers[i][0] == '\0') return TURBO_EINVAL;
    adapter->headers[i] = flow_http_client_strdup(headers[i]);
    if (!adapter->headers[i]) return TURBO_ENOMEM;
  }
  return TURBO_OK;
}

static void flow_http_client_task(coro_t *co, void *arg) {
  flow_http_client_task_t *task = (flow_http_client_task_t *)arg;
  (void)co;
  if (!task || !task->adapter || !task->adapter->client) return;
  task->response =
      http_request(task->adapter->client, flow_http_client_method_value(task->adapter->method),
                   task->adapter->url, (const char **)task->adapter->headers,
                   (int)task->adapter->header_count, task->body, task->body_len);
  task->done = 1;
}

static int flow_http_client_publish_poll_result(flow_http_client_adapter_t *adapter,
                                                const http_response_t *response) {
  turbo_flow_msg_t msg;
  const char *data = "";
  size_t len = 0;
  int rc;
  char *content_type = NULL;
  const turbo_flow_content_descriptor_t *descriptor = NULL;
  if (!adapter || !adapter->flow || !adapter->source_name) return TURBO_EINVAL;
  turbo_flow_msg_init(&msg);
  if (!response || response->error_code != HTTP_ERROR_NONE) {
    msg.status = TURBO_EIO;
  } else {
    msg.status = response->status_code;
    if (response->status_code >= adapter->success_status_min &&
        response->status_code <= adapter->success_status_max) {
      data = response->body ? response->body : "";
      len = response->body_len;
    }
  }
  msg.owned_payload = tstr_new_len(data, len);
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  if (response && response->error_code == HTTP_ERROR_NONE) {
    content_type = http_response_content_type((http_response_t *)response);
    if (content_type) {
      rc = flow_http_content_cache_get(&adapter->content_cache, content_type, &descriptor);
      free(content_type);
      if (rc != TURBO_OK && rc != TURBO_ENOENT) {
        turbo_flow_msg_cleanup(&msg);
        return rc;
      }
      if (descriptor) {
        rc = turbo_flow_msg_set_content_descriptor(&msg, descriptor);
        if (rc != TURBO_OK) {
          turbo_flow_msg_cleanup(&msg);
          return rc;
        }
      }
    }
  }
  rc = turbo_flow_publish(adapter->flow, adapter->source_name, &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static int flow_http_client_wait_for_ms(flow_http_client_adapter_t *adapter, uint32_t delay_ms) {
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

static void flow_http_client_poll_task(coro_t *co, void *arg) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)arg;
  (void)co;
  if (!adapter || !adapter->client) return;
  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    http_response_t *response =
        http_request(adapter->client, HTTP_GET, adapter->url, (const char **)adapter->headers,
                     (int)adapter->header_count, NULL, 0);
    int rc = flow_http_client_publish_poll_result(adapter, response);
    if (response) http_response_free(response);
    if (rc != TURBO_OK || !atomic_load_explicit(&adapter->started, memory_order_acquire)) break;
    if (flow_http_client_wait_for_ms(adapter, adapter->poll_interval_ms) != TURBO_OK) break;
  }
}

static void flow_http_client_loop_thread(void *arg) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)arg;
  coro_context_t *context;
  if (!adapter || !adapter->client) return;
  context = http_client_get_context(adapter->client);
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    turbo_sleep_ms(1);
  }
  if (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    (void)coro_context_run(context, TURBO_RUN_DEFAULT);
  }
}

static int flow_http_client_start(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)ctx;
  coro_context_t *context;
  if (!adapter || !stage || (adapter->poll_interval_ms > 0 && !stage->is_source) ||
      (adapter->poll_interval_ms == 0 && stage->is_source))
    return TURBO_EINVAL;
  if (adapter->poll_interval_ms == 0) {
    atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
    atomic_store_explicit(&adapter->started, 1, memory_order_release);
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
    return TURBO_OK;
  }
  tstr_freep(&adapter->source_name);
  adapter->source_name = tstr_dup(stage->name);
  if (!adapter->source_name) return TURBO_ENOMEM;
  adapter->flow = flow;
  context = http_client_get_context(adapter->client);
  if (!context) return TURBO_EINVAL;
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
  if (coro_context_spawn(context, flow_http_client_poll_task, adapter) != TURBO_OK ||
      turbo_thread_create(&adapter->loop_thread, flow_http_client_loop_thread, adapter) !=
          TURBO_OK) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_EINVAL);
    coro_context_stop(context);
    return TURBO_EINVAL;
  }
  adapter->loop_thread_started = 1;
  return TURBO_OK;
}

static int flow_http_client_consume(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)ctx;
  flow_http_client_task_t task;
  coro_context_t *context;
  uint32_t pump_limit;
  int rc = TURBO_OK;
  (void)flow;
  (void)stage;
  if (!adapter || !msg || adapter->poll_interval_ms > 0 ||
      !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    return TURBO_EINVAL;
  }
  if (atomic_load_explicit(&adapter->quiesced, memory_order_acquire)) return TURBO_EBUSY;
  context = http_client_get_context(adapter->client);
  if (!context || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;
  memset(&task, 0, sizeof(task));
  task.adapter = adapter;
  task.body = msg->payload.data ? msg->payload.data : "";
  task.body_len = msg->payload.len;
  pump_limit = adapter->max_pump_iterations ? adapter->max_pump_iterations
                                            : FLOW_HTTP_DEFAULT_PUMP_ITERATIONS;
  turbo_mutex_lock(&adapter->lock);
  tf_connection_set_usage(&adapter->connection, 0u, 1u, msg->payload.len);
  if (coro_context_spawn(context, flow_http_client_task, &task) != TURBO_OK) {
    turbo_mutex_unlock(&adapter->lock);
    tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
    return TURBO_EINVAL;
  }
  for (uint32_t pumps = 0; pumps < pump_limit && !task.done; ++pumps) {
    (void)coro_context_run(context, TURBO_RUN_ONCE);
  }
  if (!task.done) {
    rc = TURBO_ETIMEDOUT;
    while (!task.done)
      (void)coro_context_run(context, TURBO_RUN_ONCE);
  }
  if (rc == TURBO_OK && (!task.response || task.response->error_code != HTTP_ERROR_NONE)) {
    rc = TURBO_EIO;
  } else if (rc == TURBO_OK && (task.response->status_code < adapter->success_status_min ||
                                task.response->status_code > adapter->success_status_max)) {
    rc = TURBO_EPROTO;
  } else if (rc == TURBO_OK) {
    char *content_type;
    const turbo_flow_content_descriptor_t *descriptor = NULL;
    msg->status = task.response->status_code;
    rc = flow_http_client_replace_payload(msg, task.response->body, task.response->body_len);
    content_type = http_response_content_type(task.response);
    if (rc == TURBO_OK && content_type) {
      rc = flow_http_content_cache_get(&adapter->content_cache, content_type, &descriptor);
      if (rc == TURBO_ENOENT) rc = TURBO_OK;
      if (rc == TURBO_OK && descriptor) {
        rc = turbo_flow_msg_set_content_descriptor(msg, descriptor);
      }
    }
    free(content_type);
  }
  if (task.response) http_response_free(task.response);
  tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
  atomic_store_explicit(&adapter->connection.last_status, rc, memory_order_relaxed);
  turbo_mutex_unlock(&adapter->lock);
  return rc;
}

static int flow_http_client_retry_attempt(void *ctx, turbo_flow_msg_t *msg, uint32_t attempt) {
  (void)attempt;
  return flow_http_client_consume(ctx, NULL, NULL, msg);
}

static int flow_http_client_retryable(void *ctx, int status) {
  (void)ctx;
  return status == TURBO_EIO || status == TURBO_ETIMEDOUT;
}

static int flow_http_client_retry_wait(void *ctx, uint32_t delay_ms) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)ctx;
  if (!adapter) return TURBO_EINVAL;
  if (flow_http_client_wait_for_ms(adapter, delay_ms) != TURBO_OK) return TURBO_ESHUTDOWN;
  return atomic_load_explicit(&adapter->started, memory_order_acquire) ? TURBO_OK : TURBO_ESHUTDOWN;
}

static int flow_http_client_consume_retry(void *ctx, turbo_flow_t *flow,
                                          const turbo_flow_stage_plan_t *stage,
                                          turbo_flow_msg_t *msg,
                                          const turbo_flow_retry_policy_t *policy) {
  turbo_flow_retry_ops_t ops;
  (void)flow;
  (void)stage;
  memset(&ops, 0, sizeof(ops));
  ops.size = sizeof(ops);
  ops.attempt = flow_http_client_retry_attempt;
  ops.retryable = flow_http_client_retryable;
  ops.wait = flow_http_client_retry_wait;
  return turbo_flow_retry_execute(policy, msg, &ops, ctx);
}

static void flow_http_client_stop(void *ctx, turbo_flow_t *flow,
                                  const turbo_flow_stage_plan_t *stage) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)ctx;
  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_OK);
  if (adapter->wait_timer_initialized) tf_timer_signal(&adapter->wait_timer);
  if (adapter->loop_thread_started) {
    coro_context_stop(http_client_get_context(adapter->client));
    (void)turbo_thread_join(&adapter->loop_thread);
    adapter->loop_thread_started = 0;
  }
  tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_STOPPED, TURBO_ESHUTDOWN);
}

static int flow_http_client_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)ctx;
  return adapter ? tf_connection_snapshot(&adapter->connection, out) : TURBO_EINVAL;
}

static int flow_http_client_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)ctx;
  return adapter ? tf_connection_resource_metadata(&adapter->connection, adapter->resource_uid,
                                                   adapter->resource_owner,
                                                   TURBO_FLOW_DOMAIN_IO_TRANSPORT, out)
                 : TURBO_EINVAL;
}

static int flow_http_client_resource_document(void *ctx,
                                              turbo_flow_resource_document_kind_t document_kind,
                                              turbo_flow_resource_document_t *out) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)ctx;
  return adapter
             ? tf_connection_status_document(&adapter->connection, adapter->resource_uid,
                                             adapter->resource_owner,
                                             TURBO_FLOW_DOMAIN_IO_TRANSPORT,
                                             &FLOW_HTTP_RESOURCE_STATUS_SCHEMA, document_kind, out)
             : TURBO_EINVAL;
}

static int flow_http_client_command(void *ctx, turbo_flow_t *flow,
                                    const turbo_flow_adapter_command_t *command) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)ctx;
  (void)flow;
  if (!adapter || !command || !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    return TURBO_EINVAL;
  }
  if (adapter->poll_interval_ms > 0) return TURBO_ENOTSUP;
  switch (command->kind) {
  case TURBO_FLOW_ADAPTER_QUIESCE:
    atomic_store_explicit(&adapter->quiesced, 1, memory_order_release);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_RESUME:
    atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT:
    return TURBO_ENOTSUP;
  default:
    return TURBO_EINVAL;
  }
}

static void flow_http_client_shutdown(void *ctx) {
  flow_http_client_adapter_t *adapter = (flow_http_client_adapter_t *)ctx;
  if (!adapter) return;
  flow_http_client_stop(adapter, NULL, NULL);
  if (adapter->wait_timer_initialized) {
    tf_timer_destroy(&adapter->wait_timer);
    adapter->wait_timer_initialized = 0;
  }
  if (adapter->owns_client) http_client_destroy(adapter->client);
  if (adapter->lock_initialized) turbo_mutex_destroy(&adapter->lock);
  for (size_t i = 0; i < adapter->header_count; ++i)
    free(adapter->headers[i]);
  free(adapter->headers);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->resource_uid);
  tstr_freep(&adapter->resource_owner);
  tstr_freep(&adapter->url);
  tstr_freep(&adapter->bearer_token);
  flow_http_content_cache_destroy(&adapter->content_cache);
  free(adapter);
}

int turbo_flow_http_register_client_adapter(turbo_flow_t *flow, const char *name,
                                            const turbo_flow_http_client_config_t *config) {
  flow_http_client_adapter_t *adapter;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  int rc;
  if (!flow || !name || name[0] == '\0' || !config || !config->url || config->url[0] == '\0' ||
      !flow_http_client_method_valid(config->method) || config->timeout_ms < 0 ||
      config->success_status_min < 0 || config->success_status_max < 0 ||
      (config->poll_interval_ms > 0 && config->method != TURBO_FLOW_HTTP_GET)) {
    return TURBO_EINVAL;
  }
  adapter = (flow_http_client_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->quiesced, 0);
  rc = tf_connection_init(&adapter->connection, config->url, 1u);
  if (rc != TURBO_OK) {
    free(adapter);
    return rc;
  }
  adapter->method = config->method;
  adapter->success_status_min =
      config->success_status_min ? config->success_status_min : FLOW_HTTP_DEFAULT_STATUS_MIN;
  adapter->success_status_max =
      config->success_status_max ? config->success_status_max : FLOW_HTTP_DEFAULT_STATUS_MAX;
  if (adapter->success_status_min > adapter->success_status_max ||
      adapter->success_status_max > 599) {
    free(adapter);
    return TURBO_EINVAL;
  }
  adapter->url = tstr_dup(config->url);
  adapter->resource_uid = tstr_format("http:{}", name);
  adapter->resource_owner = tstr_dup(name);
  adapter->max_pump_iterations = config->max_pump_iterations;
  adapter->poll_interval_ms = config->poll_interval_ms;
  adapter->bearer_token = config->bearer_token ? tstr_dup(config->bearer_token) : NULL;
  if (!adapter->url || !adapter->resource_uid || !adapter->resource_owner ||
      (config->bearer_token && !adapter->bearer_token)) {
    flow_http_client_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  if (tstr_len(adapter->resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      tstr_len(adapter->resource_owner) > TURBO_FLOW_RESOURCE_OWNER_MAX) {
    flow_http_client_shutdown(adapter);
    return TURBO_ENAMETOOLONG;
  }
  rc = flow_http_content_cache_init(&adapter->content_cache,
                                    TURBO_FLOW_CONTENT_PROFILE_HTTP_RESPONSE_BODY, adapter->url,
                                    config->content_binding);
  if (rc != TURBO_OK) {
    flow_http_client_shutdown(adapter);
    return rc;
  }
  rc = flow_http_client_copy_headers(adapter, config->headers, config->header_count);
  if (rc != TURBO_OK) {
    flow_http_client_shutdown(adapter);
    return rc;
  }
  adapter->client = config->client ? config->client : http_client_create(NULL);
  adapter->owns_client = config->client ? config->take_client_ownership != 0 : 1;
  if (!adapter->client) {
    flow_http_client_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  if (config->timeout_ms > 0) http_client_set_timeout(adapter->client, config->timeout_ms);
  if (config->max_response_size > 0) {
    http_client_set_max_response_size(adapter->client, config->max_response_size);
  }
  if (tf_timer_init(&adapter->wait_timer) != TURBO_OK) {
    flow_http_client_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  adapter->wait_timer_initialized = 1;
  if (adapter->bearer_token) {
    http_client_set_bearer_token(adapter->client, adapter->bearer_token);
  }
  turbo_mutex_init(&adapter->lock);
  adapter->lock_initialized = 1;
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_http_client_start;
  ops.consume = flow_http_client_consume;
  if (config->poll_interval_ms == 0 &&
      (config->method == TURBO_FLOW_HTTP_GET || config->method == TURBO_FLOW_HTTP_PUT ||
       config->method == TURBO_FLOW_HTTP_DELETE)) {
    ops.consume_retry = flow_http_client_consume_retry;
  }
  ops.stop = flow_http_client_stop;
  ops.shutdown = flow_http_client_shutdown;
  ops.connection_snapshot = flow_http_client_connection_snapshot;
  ops.command = flow_http_client_command;
  resource.owner_name = name;
  resource.ops.metadata = flow_http_client_resource_metadata;
  resource.ops.document = flow_http_client_resource_document;
  resource.ctx = adapter;
  rc = turbo_flow_register_adapter_with_resources(
      flow, name, &ops, adapter,
      config->poll_interval_ms > 0 ? &FLOW_HTTP_POLL_SCHEMA : &FLOW_HTTP_CLIENT_SCHEMA, &resource,
      1u);
  return rc;
}

static int flow_http_client_resolved_method(const turbo_flow_resolved_adapter_view_t *view,
                                            turbo_flow_http_method_t *method) {
  const char *value = NULL;
  int rc = turbo_flow_resolved_adapter_get_string(view, "method", &value);
  if (rc != TURBO_OK) return rc;
  for (size_t i = 0u; i < sizeof(FLOW_HTTP_METHOD_VALUES) / sizeof(FLOW_HTTP_METHOD_VALUES[0]);
       ++i) {
    if (strcmp(value, FLOW_HTTP_METHOD_VALUES[i]) == 0) {
      *method = (turbo_flow_http_method_t)i;
      return TURBO_OK;
    }
  }
  return TURBO_EINVAL;
}

int turbo_flow_http_register_client_resolved_adapter(turbo_flow_t *flow,
                                                     const turbo_flow_resolved_config_t *resolved,
                                                     const char *adapter_name) {
  const char *headers[FLOW_HTTP_RESOLVED_HEADER_MAX] = {0};
  turbo_flow_resolved_adapter_view_t view = TURBO_FLOW_RESOLVED_ADAPTER_VIEW_INIT;
  turbo_flow_http_client_config_t config;
  int have_method = 0;
  int have_url = 0;
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
    if (strcmp(field, "url") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &config.url);
      have_url = rc == TURBO_OK;
    } else if (strcmp(field, "method") == 0) {
      rc = flow_http_client_resolved_method(&view, &config.method);
      have_method = rc == TURBO_OK;
    } else if (strcmp(field, "headers") == 0) {
      rc = turbo_flow_resolved_adapter_array_size(&view, field, &config.header_count);
      if (rc == TURBO_OK && config.header_count > FLOW_HTTP_RESOLVED_HEADER_MAX) rc = TURBO_ERANGE;
      for (size_t header = 0u; rc == TURBO_OK && header < config.header_count; ++header)
        rc = turbo_flow_resolved_adapter_array_string_at(&view, field, header, &headers[header]);
      config.headers = rc == TURBO_OK ? headers : NULL;
    } else if (strcmp(field, "bearer_token") == 0) {
      rc = turbo_flow_resolved_adapter_get_string(&view, field, &config.bearer_token);
    } else if (strcmp(field, "timeout_ms") == 0 || strcmp(field, "success_status_min") == 0 ||
               strcmp(field, "success_status_max") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && number > INT_MAX) rc = TURBO_ERANGE;
      if (rc == TURBO_OK && strcmp(field, "timeout_ms") != 0 && (number < 100u || number > 599u))
        rc = TURBO_ERANGE;
      if (rc == TURBO_OK && strcmp(field, "timeout_ms") == 0) config.timeout_ms = (int)number;
      if (rc == TURBO_OK && strcmp(field, "success_status_min") == 0)
        config.success_status_min = (int)number;
      if (rc == TURBO_OK && strcmp(field, "success_status_max") == 0)
        config.success_status_max = (int)number;
    } else if (strcmp(field, "max_response_size") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && number > SIZE_MAX) rc = TURBO_ERANGE;
      if (rc == TURBO_OK) config.max_response_size = (size_t)number;
    } else if (strcmp(field, "max_pump_iterations") == 0 ||
               strcmp(field, "poll_interval_ms") == 0) {
      rc = turbo_flow_resolved_adapter_get_u64(&view, field, &number);
      if (rc == TURBO_OK && number > UINT32_MAX) rc = TURBO_ERANGE;
      if (rc == TURBO_OK && strcmp(field, "max_pump_iterations") == 0)
        config.max_pump_iterations = (uint32_t)number;
      if (rc == TURBO_OK && strcmp(field, "poll_interval_ms") == 0)
        config.poll_interval_ms = (uint32_t)number;
    } else {
      rc = TURBO_EINVAL;
    }
    if (rc != TURBO_OK) return rc;
  }
  if (!have_url || !have_method) return TURBO_EINVAL;
  return turbo_flow_http_register_client_adapter(flow, adapter_name, &config);
}
