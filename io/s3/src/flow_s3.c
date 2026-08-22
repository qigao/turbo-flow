#include "turbo_flow_s3.h"

#include "CoroNet.h"
#include "flow_connection.h"
#include "flow_resource_status.h"
#include "flow_timer.h"
#include "fmt.h"
#include "s3/s3_client.h"
#include "s3/s3_credentials.h"
#include "s3/s3_error.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_S3_DEFAULT_PUMP_ITERATIONS 10000u
#define FLOW_S3_DEFAULT_CONTENT_TYPE "application/octet-stream"

typedef struct flow_s3_adapter_s flow_s3_adapter_t;

typedef struct flow_s3_task_s {
  flow_s3_adapter_t *adapter;
  const char *data;
  size_t len;
  tstr result;
  s3_stat_object_response_t stat;
  turbo_flow_content_descriptor_t content_descriptor;
  s3_error_t error;
  int content_status;
  int has_content_descriptor;
  int get;
  int done;
} flow_s3_task_t;

struct flow_s3_adapter_s {
  turbo_flow_t *flow;
  tstr source_name;
  tstr resource_uid;
  tstr resource_owner;
  tstr host;
  tstr region;
  tstr bucket;
  tstr object;
  tstr content_type;
  tstr content_identity;
  turbo_flow_content_binding_t content_binding;
  char content_schema_name[TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX + 1u];
  char content_type_name[TURBO_FLOW_CONTENT_TYPE_NAME_MAX + 1u];
  uint16_t port;
  int use_https;
  int virtual_style;
  uint32_t poll_interval_ms;
  uint32_t max_pump_iterations;
  coro_context_t *context;
  s3_credential_provider_t *provider;
  s3_client_t *client;
  atomic_int started;
  atomic_int quiesced;
  tf_connection_state_t connection;
  int connection_initialized;
  tf_timer_t poll_wait;
  int poll_wait_initialized;
  turbo_mutex_t lock;
  int lock_initialized;
  int loop_thread_started;
  turbo_thread_t loop_thread;
  turbo_flow_content_descriptor_t content_descriptor;
  int has_content_descriptor;
};

static const char *const FLOW_S3_CREDENTIAL_VALUES[] = {"static", "aws_env", "minio_env"};
static const turbo_flow_option_field_t FLOW_S3_FIELDS[] = {
    {"host", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"port", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX, 1, 65535,
     NULL, 0},
    {"use_https", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"region", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"virtual_style", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"bucket", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"object", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"content_type", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"credentials", TURBO_FLOW_OPTION_ENUM, TURBO_FLOW_OPTION_REQUIRED, 0, 0,
     FLOW_S3_CREDENTIAL_VALUES, 3},
    {"access_key", TURBO_FLOW_OPTION_SECRET, TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"secret_key", TURBO_FLOW_OPTION_SECRET, TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"session_token", TURBO_FLOW_OPTION_SECRET, TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"max_pump_iterations", TURBO_FLOW_OPTION_U32, 0, 0, 0, NULL, 0},
    {"poll_interval_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"content_binding", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0,
     NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_S3_SINK_SCHEMA = {NULL,
                                                                TURBO_FLOW_ADAPTER_KIND_S3,
                                                                TURBO_FLOW_ADAPTER_SINK,
                                                                TURBO_FLOW_ADAPTER_OUTPUT,
                                                                FLOW_S3_FIELDS,
                                                                sizeof(FLOW_S3_FIELDS) /
                                                                    sizeof(FLOW_S3_FIELDS[0])};
static const turbo_flow_adapter_schema_t FLOW_S3_POLL_SCHEMA = {NULL,
                                                                TURBO_FLOW_ADAPTER_KIND_S3,
                                                                TURBO_FLOW_ADAPTER_SOURCE,
                                                                TURBO_FLOW_ADAPTER_INPUT,
                                                                FLOW_S3_FIELDS,
                                                                sizeof(FLOW_S3_FIELDS) /
                                                                    sizeof(FLOW_S3_FIELDS[0])};

static const char FLOW_S3_RESOURCE_SCHEMA_TEXT[] =
    "schema TurboFlowS3Resource [id(201), version(1)];\n"
    "message S3ClientStatus {\n"
    "  uint32 state;\n"
    "  int32 last_status;\n"
    "  string connections_current;\n"
    "  string connection_limit;\n"
    "  string in_flight_messages;\n"
    "  string in_flight_bytes;\n"
    "}\n";

static const turbo_flow_resource_schema_t FLOW_S3_RESOURCE_STATUS_SCHEMA = {
    sizeof(turbo_flow_resource_schema_t),
    TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
    TURBO_FLOW_RESOURCE_CONNECTION,
    TURBO_FLOW_RESOURCE_DOCUMENT_STATUS,
    TURBO_FLOW_RESOURCE_DOCUMENT_JSON,
    "TurboFlowS3Resource",
    "S3ClientStatus",
    201u,
    1u,
    FLOW_S3_RESOURCE_SCHEMA_TEXT};

static int flow_s3_collect(const char *data, size_t len, void *userdata) {
  flow_s3_task_t *task = (flow_s3_task_t *)userdata;
  tstr next;
  if (!task || (len > 0 && !data)) return TURBO_EINVAL;
  next = tstr_cat_len(task->result, data ? data : "", len);
  if (!next) return TURBO_ENOMEM;
  task->result = next;
  return TURBO_OK;
}

static void flow_s3_task_cleanup(flow_s3_task_t *task) {
  if (!task) return;
  tstr_freep(&task->result);
  s3_stat_object_response_free(&task->stat);
  s3_error_free(&task->error);
}

static void flow_s3_request_task(coro_t *co, void *arg) {
  flow_s3_task_t *task = (flow_s3_task_t *)arg;
  (void)co;
  if (!task || !task->adapter || !task->adapter->client) return;
  if (task->get) {
    task->stat =
        s3_stat_object(task->adapter->client, task->adapter->bucket, task->adapter->object);
    if (!s3_is_ok(task->stat.error)) {
      task->error = task->stat.error;
      task->stat.error = S3_OK;
      task->done = 1;
      return;
    }
    if (task->stat.content_type && task->stat.content_type[0] != '\0') {
      task->content_status = turbo_flow_content_descriptor_from_media(
          &task->content_descriptor, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
          TURBO_FLOW_CONTENT_PROFILE_S3_OBJECT, task->stat.content_type,
          task->adapter->content_identity, &task->adapter->content_binding);
      if (task->content_status == TURBO_OK) {
        task->has_content_descriptor = 1;
      } else if (task->content_status == TURBO_ENOENT &&
                 task->adapter->content_binding.schema.schema_version == 0u) {
        task->content_status = TURBO_OK;
      } else {
        if (task->content_status == TURBO_ENOENT) task->content_status = TURBO_EPROTO;
        task->done = 1;
        return;
      }
    }
    task->result = tstr_new_len("", 0);
    if (!task->result) {
      task->error = s3_error_make(TURBO_ENOMEM, "out of memory");
    } else {
      task->error = s3_get_object(task->adapter->client, task->adapter->bucket,
                                  task->adapter->object, flow_s3_collect, task);
    }
  } else {
    task->error = s3_put_object(task->adapter->client, task->adapter->bucket, task->adapter->object,
                                task->data, task->len, task->adapter->content_type);
  }
  task->done = 1;
}

static int flow_s3_request_begin(flow_s3_adapter_t *adapter, size_t bytes) {
  if (!adapter) return TURBO_EINVAL;
  return tf_connection_request_begin(&adapter->connection, (uint64_t)bytes);
}

static void flow_s3_request_end(flow_s3_adapter_t *adapter, size_t bytes, int status) {
  int end_rc;
  if (!adapter) return;
  end_rc = tf_connection_request_end(&adapter->connection, (uint64_t)bytes);
  atomic_store_explicit(&adapter->connection.last_status, end_rc == TURBO_OK ? status : end_rc,
                        memory_order_relaxed);
}

static int flow_s3_run(flow_s3_adapter_t *adapter, flow_s3_task_t *task) {
  uint32_t limit =
      adapter->max_pump_iterations ? adapter->max_pump_iterations : FLOW_S3_DEFAULT_PUMP_ITERATIONS;
  uint32_t pumps = 0;
  if (coro_context_spawn(adapter->context, flow_s3_request_task, task) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  for (; pumps < limit && !task->done; ++pumps) {
    (void)coro_context_run(adapter->context, TURBO_RUN_ONCE);
  }
  if (task->done) return TURBO_OK;
  while (!task->done)
    (void)coro_context_run(adapter->context, TURBO_RUN_ONCE);
  return TURBO_ETIMEDOUT;
}

static int flow_s3_publish(flow_s3_adapter_t *adapter, flow_s3_task_t *task, int run_rc) {
  turbo_flow_msg_t msg;
  int rc;
  turbo_flow_msg_init(&msg);
  if (run_rc == TURBO_OK && task->content_status != TURBO_OK) {
    return task->content_status;
  }
  if (run_rc == TURBO_OK && s3_is_ok(task->error) && task->result) {
    msg.owned_payload = task->result;
    task->result = NULL;
    msg.status = TURBO_OK;
  } else {
    msg.owned_payload = tstr_new_len("", 0);
    msg.status =
        run_rc != TURBO_OK ? run_rc : (task->error.code != 0 ? task->error.code : TURBO_EIO);
  }
  if (!msg.owned_payload) return TURBO_ENOMEM;
  msg.payload = tstr_to_v(msg.owned_payload);
  if (msg.status == TURBO_OK && task->has_content_descriptor) {
    rc = turbo_flow_msg_set_content_descriptor(&msg, &task->content_descriptor);
    if (rc != TURBO_OK) {
      turbo_flow_msg_cleanup(&msg);
      return rc;
    }
  }
  rc = turbo_flow_publish(adapter->flow, adapter->source_name, &msg);
  turbo_flow_msg_cleanup(&msg);
  return rc;
}

static void flow_s3_loop_thread(void *arg) {
  flow_s3_adapter_t *adapter = (flow_s3_adapter_t *)arg;
  if (!adapter) return;
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    turbo_sleep_ms(1);
  }
  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    flow_s3_task_t task;
    int run_rc;
    memset(&task, 0, sizeof(task));
    task.adapter = adapter;
    task.get = 1;
    if (flow_s3_request_begin(adapter, 0u) != TURBO_OK) break;
    turbo_mutex_lock(&adapter->lock);
    run_rc = flow_s3_run(adapter, &task);
    turbo_mutex_unlock(&adapter->lock);
    flow_s3_request_end(adapter, 0u,
                        run_rc != TURBO_OK ? run_rc
                                           : (task.content_status != TURBO_OK
                                                  ? task.content_status
                                                  : (s3_is_ok(task.error) ? TURBO_OK : TURBO_EIO)));
    if (flow_s3_publish(adapter, &task, run_rc) != TURBO_OK) {
      flow_s3_task_cleanup(&task);
      break;
    }
    flow_s3_task_cleanup(&task);
    if (tf_timer_wait_for_ms(&adapter->poll_wait, adapter->poll_interval_ms) == TURBO_ESHUTDOWN) {
      break;
    }
  }
}

static int flow_s3_start(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_s3_adapter_t *adapter = (flow_s3_adapter_t *)ctx;
  if (!adapter || !stage || (adapter->poll_interval_ms > 0 && !stage->is_source) ||
      (adapter->poll_interval_ms == 0 && stage->is_source))
    return TURBO_EINVAL;
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_OK);
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
  tf_timer_reset(&adapter->poll_wait);
  if (adapter->poll_interval_ms == 0) return TURBO_OK;
  adapter->flow = flow;
  tstr_freep(&adapter->source_name);
  adapter->source_name = tstr_dup(stage->name);
  if (!adapter->source_name) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_ENOMEM);
    return TURBO_ENOMEM;
  }
  if (turbo_thread_create(&adapter->loop_thread, flow_s3_loop_thread, adapter) != TURBO_OK) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_EINVAL);
    return TURBO_EINVAL;
  }
  adapter->loop_thread_started = 1;
  return TURBO_OK;
}

static int flow_s3_consume(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage,
                           turbo_flow_msg_t *msg) {
  flow_s3_adapter_t *adapter = (flow_s3_adapter_t *)ctx;
  flow_s3_task_t task;
  int rc;
  (void)flow;
  (void)stage;
  if (!adapter || !msg || adapter->poll_interval_ms > 0 ||
      !atomic_load_explicit(&adapter->started, memory_order_acquire) ||
      (msg->payload.len > 0 && !msg->payload.data))
    return TURBO_EINVAL;
  if (atomic_load_explicit(&adapter->quiesced, memory_order_acquire)) return TURBO_EBUSY;
  if (adapter->has_content_descriptor) {
    const turbo_flow_content_descriptor_t *actual = turbo_flow_msg_content_descriptor(msg);
    rc = actual
             ? turbo_flow_content_descriptor_validate_payload(&adapter->content_descriptor, actual)
             : turbo_flow_msg_set_content_descriptor(msg, &adapter->content_descriptor);
    if (rc != TURBO_OK) return rc;
  }
  memset(&task, 0, sizeof(task));
  task.adapter = adapter;
  task.data = msg->payload.data ? msg->payload.data : "";
  task.len = msg->payload.len;
  rc = flow_s3_request_begin(adapter, task.len);
  if (rc != TURBO_OK) return rc;
  turbo_mutex_lock(&adapter->lock);
  rc = flow_s3_run(adapter, &task);
  turbo_mutex_unlock(&adapter->lock);
  if (rc == TURBO_OK && !s3_is_ok(task.error)) rc = TURBO_EIO;
  if (task.error.code != 0) msg->status = task.error.code;
  flow_s3_request_end(adapter, task.len, rc);
  s3_error_free(&task.error);
  return rc;
}

static void flow_s3_stop(void *ctx, turbo_flow_t *flow, const turbo_flow_stage_plan_t *stage) {
  flow_s3_adapter_t *adapter = (flow_s3_adapter_t *)ctx;
  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  if (adapter->connection_initialized) {
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_OK);
  }
  if (adapter->poll_wait_initialized) tf_timer_stop(&adapter->poll_wait);
  if (adapter->loop_thread_started) {
    (void)turbo_thread_join(&adapter->loop_thread);
    adapter->loop_thread_started = 0;
  }
  if (adapter->connection_initialized) {
    tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_STOPPED, TURBO_ESHUTDOWN);
  }
}

static int flow_s3_connection_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flow_s3_adapter_t *adapter = (flow_s3_adapter_t *)ctx;
  return adapter && adapter->connection_initialized
             ? tf_connection_snapshot(&adapter->connection, out)
             : TURBO_EINVAL;
}

static int flow_s3_resource_metadata(void *ctx, turbo_flow_resource_metadata_t *out) {
  flow_s3_adapter_t *adapter = (flow_s3_adapter_t *)ctx;
  return adapter && adapter->connection_initialized
             ? tf_connection_resource_metadata(
                   &adapter->connection, adapter->resource_uid, adapter->resource_owner,
                   TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, out)
             : TURBO_EINVAL;
}

static int flow_s3_resource_document(void *ctx,
                                     turbo_flow_resource_document_kind_t document_kind,
                                     turbo_flow_resource_document_t *out) {
  flow_s3_adapter_t *adapter = (flow_s3_adapter_t *)ctx;
  return adapter && adapter->connection_initialized
             ? tf_connection_status_document(
                   &adapter->connection, adapter->resource_uid, adapter->resource_owner,
                   TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE, &FLOW_S3_RESOURCE_STATUS_SCHEMA,
                   document_kind, out)
             : TURBO_EINVAL;
}

static int flow_s3_command(void *ctx, turbo_flow_t *flow,
                           const turbo_flow_adapter_command_t *command) {
  flow_s3_adapter_t *adapter = (flow_s3_adapter_t *)ctx;
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

static void flow_s3_shutdown(void *ctx) {
  flow_s3_adapter_t *adapter = (flow_s3_adapter_t *)ctx;
  if (!adapter) return;
  flow_s3_stop(adapter, NULL, NULL);
  s3_client_destroy(adapter->client);
  s3_credential_provider_destroy(adapter->provider);
  if (adapter->context) coro_context_destroy(adapter->context);
  if (adapter->poll_wait_initialized) tf_timer_destroy(&adapter->poll_wait);
  if (adapter->lock_initialized) turbo_mutex_destroy(&adapter->lock);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->resource_uid);
  tstr_freep(&adapter->resource_owner);
  tstr_freep(&adapter->host);
  tstr_freep(&adapter->region);
  tstr_freep(&adapter->bucket);
  tstr_freep(&adapter->object);
  tstr_freep(&adapter->content_type);
  tstr_freep(&adapter->content_identity);
  free(adapter);
}

static int flow_s3_binding_copy(flow_s3_adapter_t *adapter,
                                const turbo_flow_content_binding_t *binding) {
  int fields;
  size_t len;
  if (!adapter) return TURBO_EINVAL;
  adapter->content_binding = (turbo_flow_content_binding_t)TURBO_FLOW_CONTENT_BINDING_INIT;
  if (!binding) return TURBO_OK;
  if (binding->size < sizeof(*binding) || binding->schema.size < sizeof(binding->schema)) {
    return TURBO_EINVAL;
  }
  fields = (binding->schema.schema_name && binding->schema.schema_name[0]) +
           (binding->schema.type_name && binding->schema.type_name[0]) +
           (binding->schema.schema_version != 0u);
  if (fields != 0 && fields != 3) return TURBO_EINVAL;
  if (fields == 3 && !binding->registry) return TURBO_EINVAL;
  adapter->content_binding.registry = binding->registry;
  if (fields == 0) return TURBO_OK;
  len = strlen(binding->schema.schema_name);
  if (len > TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX) return TURBO_ENOSPC;
  memcpy(adapter->content_schema_name, binding->schema.schema_name, len + 1u);
  len = strlen(binding->schema.type_name);
  if (len > TURBO_FLOW_CONTENT_TYPE_NAME_MAX) return TURBO_ENOSPC;
  memcpy(adapter->content_type_name, binding->schema.type_name, len + 1u);
  adapter->content_binding.schema.schema_name = adapter->content_schema_name;
  adapter->content_binding.schema.type_name = adapter->content_type_name;
  adapter->content_binding.schema.schema_version = binding->schema.schema_version;
  return TURBO_OK;
}

static s3_credential_provider_t *flow_s3_create_provider(const turbo_flow_s3_config_t *config) {
  switch (config->credentials) {
  case TURBO_FLOW_S3_CREDENTIALS_STATIC:
    if (!config->access_key || !config->secret_key) return NULL;
    return s3_creds_static(config->access_key, config->secret_key, config->session_token);
  case TURBO_FLOW_S3_CREDENTIALS_AWS_ENV:
    return s3_creds_env_aws();
  case TURBO_FLOW_S3_CREDENTIALS_MINIO_ENV:
    return s3_creds_env_minio();
  default:
    return NULL;
  }
}

int turbo_flow_s3_register_client_adapter(turbo_flow_t *flow, const char *name,
                                          const turbo_flow_s3_config_t *config) {
  flow_s3_adapter_t *adapter;
  turbo_flow_adapter_ops_t ops;
  turbo_flow_resource_provider_registration_t resource =
      TURBO_FLOW_RESOURCE_PROVIDER_REGISTRATION_INIT;
  s3_base_url_t base_url;
  tstr endpoint = NULL;
  int rc;
  if (!flow || !name || name[0] == '\0' || !config || !config->host || config->host[0] == '\0' ||
      config->port == 0 || !config->region || config->region[0] == '\0' || !config->bucket ||
      config->bucket[0] == '\0' || !config->object || config->object[0] == '\0' ||
      config->credentials < TURBO_FLOW_S3_CREDENTIALS_STATIC ||
      config->credentials > TURBO_FLOW_S3_CREDENTIALS_MINIO_ENV ||
      (config->credentials == TURBO_FLOW_S3_CREDENTIALS_STATIC &&
       (!config->access_key || config->access_key[0] == '\0' || !config->secret_key ||
        config->secret_key[0] == '\0')))
    return TURBO_EINVAL;
  adapter = (flow_s3_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->quiesced, 0);
  adapter->host = tstr_dup(config->host);
  adapter->resource_uid = tstr_format("s3:{}", name);
  adapter->resource_owner = tstr_dup(name);
  adapter->region = tstr_dup(config->region);
  adapter->bucket = tstr_dup(config->bucket);
  adapter->object = tstr_dup(config->object);
  adapter->content_type =
      tstr_dup(config->content_type ? config->content_type : FLOW_S3_DEFAULT_CONTENT_TYPE);
  adapter->content_identity = tstr_format("{}/{}", adapter->bucket, adapter->object);
  adapter->port = config->port;
  adapter->use_https = config->use_https != 0;
  adapter->virtual_style = config->virtual_style != 0;
  adapter->poll_interval_ms = config->poll_interval_ms;
  adapter->max_pump_iterations = config->max_pump_iterations;
  if (!adapter->host || !adapter->resource_uid || !adapter->resource_owner || !adapter->region ||
      !adapter->bucket || !adapter->object || !adapter->content_type ||
      !adapter->content_identity) {
    flow_s3_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  if (tstr_len(adapter->resource_uid) > TURBO_FLOW_RESOURCE_UID_MAX ||
      tstr_len(adapter->resource_owner) > TURBO_FLOW_RESOURCE_OWNER_MAX) {
    flow_s3_shutdown(adapter);
    return TURBO_ENAMETOOLONG;
  }
  rc = flow_s3_binding_copy(adapter, config->content_binding);
  if (rc != TURBO_OK) {
    flow_s3_shutdown(adapter);
    return rc;
  }
  if (config->poll_interval_ms == 0u) {
    rc = turbo_flow_content_descriptor_from_media(
        &adapter->content_descriptor, TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE,
        TURBO_FLOW_CONTENT_PROFILE_S3_OBJECT, adapter->content_type, adapter->content_identity,
        &adapter->content_binding);
    if (rc == TURBO_OK) {
      adapter->has_content_descriptor = 1;
    } else if (rc != TURBO_ENOENT || adapter->content_binding.schema.schema_version != 0u) {
      flow_s3_shutdown(adapter);
      return rc == TURBO_ENOENT ? TURBO_EPROTO : rc;
    }
  }
  endpoint = tstr_format("{}://{}:{}/{}/{}", adapter->use_https ? "https" : "http", adapter->host,
                         (unsigned int)adapter->port, adapter->bucket, adapter->object);
  if (!endpoint) {
    flow_s3_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  rc = tf_connection_init(&adapter->connection, endpoint, 1u);
  tstr_freep(&endpoint);
  if (rc != TURBO_OK) {
    flow_s3_shutdown(adapter);
    return rc;
  }
  adapter->connection_initialized = 1;
  adapter->context = coro_context_create(NULL);
  adapter->provider = flow_s3_create_provider(config);
  if (!adapter->context || !adapter->provider) {
    flow_s3_shutdown(adapter);
    return config->credentials == TURBO_FLOW_S3_CREDENTIALS_STATIC &&
                   (!config->access_key || !config->secret_key)
               ? TURBO_EINVAL
               : TURBO_ENOMEM;
  }
  memset(&base_url, 0, sizeof(base_url));
  base_url.host = adapter->host;
  base_url.port = adapter->port;
  base_url.is_https = adapter->use_https;
  base_url.region = adapter->region;
  base_url.virtual_style = adapter->virtual_style;
  adapter->client = s3_client_create(adapter->context, &base_url, adapter->provider);
  if (!adapter->client) {
    flow_s3_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  turbo_mutex_init(&adapter->lock);
  adapter->lock_initialized = 1;
  if (tf_timer_init(&adapter->poll_wait) != TURBO_OK) {
    flow_s3_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  adapter->poll_wait_initialized = 1;
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_s3_start;
  ops.consume = flow_s3_consume;
  ops.stop = flow_s3_stop;
  ops.shutdown = flow_s3_shutdown;
  ops.connection_snapshot = flow_s3_connection_snapshot;
  ops.command = flow_s3_command;
  resource.owner_name = name;
  resource.ops.metadata = flow_s3_resource_metadata;
  resource.ops.document = flow_s3_resource_document;
  resource.ctx = adapter;
  rc = turbo_flow_register_adapter_with_resources(
      flow, name, &ops, adapter,
      config->poll_interval_ms > 0 ? &FLOW_S3_POLL_SCHEMA : &FLOW_S3_SINK_SCHEMA, &resource, 1u);
  return rc;
}
