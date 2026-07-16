#include "turbo_flow_email.h"

#include "email/email_message.h"
#include "email/email_smtp.h"
#include "flow_connection.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *const FLOW_EMAIL_AUTH_VALUES[] = {"none", "plain", "login", "cram_md5"};
static const turbo_flow_option_field_t FLOW_EMAIL_OPTION_FIELDS[] = {
    {"host", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"port", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX,
     1, 65535, NULL, 0},
    {"use_tls", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"use_starttls", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"auth_method", TURBO_FLOW_OPTION_ENUM, 0, 0, 0, FLOW_EMAIL_AUTH_VALUES, 4},
    {"username", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"password", TURBO_FLOW_OPTION_SECRET, TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"from_name", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"from_email", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"to_name", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"to_email", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"subject", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"html_body", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"max_pump_iterations", TURBO_FLOW_OPTION_U32, 0, 0, 0, NULL, 0},
    {"context", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE,
     0, 0, NULL, 0},
    {"take_context_ownership", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0}};

static const turbo_flow_adapter_schema_t FLOW_EMAIL_SMTP_SCHEMA = {
    NULL, TURBO_FLOW_ADAPTER_KIND_EMAIL, TURBO_FLOW_ADAPTER_SINK,
    TURBO_FLOW_ADAPTER_OUTPUT, FLOW_EMAIL_OPTION_FIELDS,
    sizeof(FLOW_EMAIL_OPTION_FIELDS) / sizeof(FLOW_EMAIL_OPTION_FIELDS[0])};

#define FLOW_EMAIL_DEFAULT_PUMP_ITERATIONS 20000u
#define FLOW_EMAIL_MESSAGE_POOL_BYTES 16384u

typedef struct flow_email_smtp_adapter_s {
  coro_context_t *ctx;
  tstr_t host;
  tstr_t username;
  tstr_t password;
  tstr_t from_name;
  tstr_t from_email;
  tstr_t to_name;
  tstr_t to_email;
  tstr_t subject;
  int port;
  int use_tls;
  int use_starttls;
  smtp_auth_method_t auth_method;
  int timeout_ms;
  int html_body;
  uint32_t max_pump_iterations;
  int owns_context;
  atomic_int started;
  atomic_int quiesced;
  tf_connection_state_t connection;
  turbo_mutex_t request_mutex;
  struct flow_email_send_task_s *active_requests;
  size_t active_request_count;
  size_t active_request_bytes;
  int request_mutex_initialized;
} flow_email_smtp_adapter_t;

typedef struct flow_email_send_task_s {
  flow_email_smtp_adapter_t *adapter;
  struct flow_email_send_task_s *next;
  tstr_t payload;
  smtp_client_t *smtp;
  atomic_int refs;
  atomic_int done;
  int status;
} flow_email_send_task_t;

static void flow_email_send_task_release(flow_email_send_task_t *task) {
  if (!task || atomic_fetch_sub_explicit(&task->refs, 1, memory_order_acq_rel) != 1) return;
  tstr_freep(&task->payload);
  free(task);
}

static void flow_email_send_task_complete(flow_email_send_task_t *task, int status) {
  flow_email_smtp_adapter_t *adapter;
  flow_email_send_task_t **link;
  if (!task || !(adapter = task->adapter)) return;
  turbo_mutex_lock(&adapter->request_mutex);
  link = &adapter->active_requests;
  while (*link && *link != task) link = &(*link)->next;
  if (*link == task) {
    *link = task->next;
    adapter->active_request_count -= 1u;
    adapter->active_request_bytes -= tstr_len(task->payload);
  }
  task->status = status;
  tf_connection_set_usage(&adapter->connection, 0u, adapter->active_request_count,
                          adapter->active_request_bytes);
  atomic_store_explicit(&adapter->connection.last_status, status, memory_order_relaxed);
  atomic_store_explicit(&task->done, 1, memory_order_release);
  turbo_mutex_unlock(&adapter->request_mutex);
}

static int flow_email_smtp_is_configured(const flow_email_smtp_adapter_t *adapter) {
  return adapter && adapter->ctx && adapter->host && adapter->host[0] != '\0' &&
         adapter->port > 0 && adapter->from_email && adapter->from_email[0] != '\0' &&
         adapter->to_email && adapter->to_email[0] != '\0';
}

static int flow_email_dup_opt(tstr_t *dst, const char *src) {
  if (!dst) return TURBO_EINVAL;
  if (!src) return TURBO_OK;
  *dst = tstr_dup(src);
  return *dst ? TURBO_OK : TURBO_ENOMEM;
}

static int flow_email_payload_to_cstr(const char *data, size_t len, tstr_t *out) {
  if (!out) return TURBO_EINVAL;
  if (len > 0 && !data) return TURBO_EINVAL;
  *out = tstr_new_len(data ? data : "", len);
  return *out ? TURBO_OK : TURBO_ENOMEM;
}

static int flow_email_send_message(flow_email_send_task_t *task) {
  flow_email_smtp_adapter_t *adapter = task ? task->adapter : NULL;
  smtp_config_t smtp_config;
  smtp_client_t *smtp = NULL;
  mem_pool_t pool;
  email_message_t *msg = NULL;
  tstr_t body = NULL;
  int rc = TURBO_EINVAL;

  if (!task || !flow_email_smtp_is_configured(adapter)) return TURBO_ENOTSUP;
  rc = flow_email_payload_to_cstr(task->payload, tstr_len(task->payload), &body);
  if (rc != TURBO_OK) return rc;

  memset(&smtp_config, 0, sizeof(smtp_config));
  smtp_config.host = adapter->host;
  smtp_config.port = adapter->port;
  smtp_config.use_tls = adapter->use_tls;
  smtp_config.use_starttls = adapter->use_starttls;
  smtp_config.auth_method = adapter->auth_method;
  smtp_config.username = adapter->username;
  smtp_config.password = adapter->password;
  smtp_config.timeout_ms = adapter->timeout_ms;

  smtp = smtp_client_create(adapter->ctx, &smtp_config);
  if (!smtp) {
    tstr_freep(&body);
    return TURBO_ENOMEM;
  }
  turbo_mutex_lock(&adapter->request_mutex);
  task->smtp = smtp;
  turbo_mutex_unlock(&adapter->request_mutex);

  if (smtp_connect(smtp) != 0) {
    turbo_mutex_lock(&adapter->request_mutex);
    task->smtp = NULL;
    turbo_mutex_unlock(&adapter->request_mutex);
    smtp_client_free(smtp);
    tstr_freep(&body);
    return TURBO_EIO;
  }

  mem_init(&pool, FLOW_EMAIL_MESSAGE_POOL_BYTES);
  msg = email_message_create(&pool);
  if (!msg) {
    mem_destroy(&pool);
    smtp_disconnect(smtp);
    smtp_client_free(smtp);
    tstr_freep(&body);
    return TURBO_ENOMEM;
  }

  if (email_message_set_from(msg, adapter->from_name, adapter->from_email) != 0 ||
      email_message_add_to(msg, adapter->to_name, adapter->to_email) != 0 ||
      email_message_set_subject(msg, adapter->subject ? adapter->subject : "") != 0 ||
      (adapter->html_body ? email_message_set_html_body(msg, body)
                          : email_message_set_text_body(msg, body)) != 0) {
    rc = TURBO_EINVAL;
  } else if (smtp_send_message(smtp, msg) != 0) {
    rc = TURBO_EIO;
  } else {
    rc = TURBO_OK;
  }

  email_message_free(msg);
  mem_destroy(&pool);
  smtp_disconnect(smtp);
  turbo_mutex_lock(&adapter->request_mutex);
  task->smtp = NULL;
  turbo_mutex_unlock(&adapter->request_mutex);
  smtp_client_free(smtp);
  tstr_freep(&body);
  return rc;
}

static void flow_email_send_task(coro_t *co, void *arg) {
  flow_email_send_task_t *task = (flow_email_send_task_t *)arg;

  (void)co;
  if (!task || !task->adapter) return;
  flow_email_send_task_complete(task, flow_email_send_message(task));
  flow_email_send_task_release(task);
}

static int flow_email_smtp_start(void *ctx,
                                 turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flow_email_smtp_adapter_t *adapter = (flow_email_smtp_adapter_t *)ctx;

  (void)flow;
  if (!adapter || !stage) return TURBO_EINVAL;
  if (stage->is_source) return TURBO_EINVAL;
  if (!flow_email_smtp_is_configured(adapter)) return TURBO_ENOTSUP;
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
  return TURBO_OK;
}

static int flow_email_smtp_consume(void *ctx,
                                   turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage,
                                   turbo_flow_msg_t *msg) {
  flow_email_smtp_adapter_t *adapter = (flow_email_smtp_adapter_t *)ctx;
  flow_email_send_task_t *task;
  int rc;
  uint32_t pump_limit;

  (void)flow;
  (void)stage;
  if (!adapter || !adapter->ctx) return TURBO_EINVAL;
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) return TURBO_ESHUTDOWN;
  if (atomic_load_explicit(&adapter->quiesced, memory_order_acquire)) return TURBO_EBUSY;
  if (!msg || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;

  task = (flow_email_send_task_t *)calloc(1, sizeof(*task));
  if (!task) return TURBO_ENOMEM;
  task->adapter = adapter;
  task->payload = tstr_new_len(msg->payload.data ? msg->payload.data : "", msg->payload.len);
  if (!task->payload) {
    free(task);
    return TURBO_ENOMEM;
  }
  task->status = TURBO_EALREADY;
  atomic_init(&task->refs, 2);
  atomic_init(&task->done, 0);

  turbo_mutex_lock(&adapter->request_mutex);
  if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    turbo_mutex_unlock(&adapter->request_mutex);
    flow_email_send_task_release(task);
    flow_email_send_task_release(task);
    return TURBO_ESHUTDOWN;
  }
  task->next = adapter->active_requests;
  adapter->active_requests = task;
  adapter->active_request_count += 1u;
  adapter->active_request_bytes += msg->payload.len;
  tf_connection_set_usage(&adapter->connection, 0u, adapter->active_request_count,
                          adapter->active_request_bytes);
  turbo_mutex_unlock(&adapter->request_mutex);

  rc = coro_context_spawn(adapter->ctx, flow_email_send_task, task);
  if (rc != TURBO_OK) {
    flow_email_send_task_complete(task, rc);
    flow_email_send_task_release(task);
    flow_email_send_task_release(task);
    return rc;
  }

  pump_limit = adapter->max_pump_iterations ? adapter->max_pump_iterations
                                            : FLOW_EMAIL_DEFAULT_PUMP_ITERATIONS;
  for (uint32_t i = 0;
       i < pump_limit && !atomic_load_explicit(&task->done, memory_order_acquire); ++i) {
    (void)coro_context_run(adapter->ctx, TURBO_RUN_ONCE);
  }

  rc = atomic_load_explicit(&task->done, memory_order_acquire) ? task->status : TURBO_ETIMEDOUT;
  flow_email_send_task_release(task);
  return rc;
}

static void flow_email_smtp_drain_requests(flow_email_smtp_adapter_t *adapter) {
  flow_email_send_task_t *task;
  if (!adapter || !adapter->request_mutex_initialized) return;
  turbo_mutex_lock(&adapter->request_mutex);
  for (task = adapter->active_requests; task; task = task->next) {
    if (task->smtp) (void)smtp_interrupt(task->smtp, TURBO_ESHUTDOWN);
  }
  turbo_mutex_unlock(&adapter->request_mutex);
  for (;;) {
    size_t active;
    turbo_mutex_lock(&adapter->request_mutex);
    active = adapter->active_request_count;
    turbo_mutex_unlock(&adapter->request_mutex);
    if (active == 0u) return;
    (void)coro_context_run(adapter->ctx, TURBO_RUN_ONCE);
  }
}

static void flow_email_smtp_stop(void *ctx,
                                 turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flow_email_smtp_adapter_t *adapter = (flow_email_smtp_adapter_t *)ctx;

  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_OK);
  flow_email_smtp_drain_requests(adapter);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_STOPPED,
                           TURBO_ESHUTDOWN);
}

static int flow_email_smtp_connection_snapshot(void *ctx,
                                               turbo_flow_connection_snapshot_t *out) {
  flow_email_smtp_adapter_t *adapter = (flow_email_smtp_adapter_t *)ctx;
  return adapter ? tf_connection_snapshot(&adapter->connection, out) : TURBO_EINVAL;
}

static int flow_email_smtp_command(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_adapter_command_t *command) {
  flow_email_smtp_adapter_t *adapter = (flow_email_smtp_adapter_t *)ctx;
  (void)flow;
  if (!adapter || !command || !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    return TURBO_EINVAL;
  }
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

static void flow_email_smtp_shutdown(void *ctx) {
  flow_email_smtp_adapter_t *adapter = (flow_email_smtp_adapter_t *)ctx;

  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  flow_email_smtp_drain_requests(adapter);
  if (adapter->owns_context && adapter->ctx) {
    coro_context_stop(adapter->ctx);
    coro_context_destroy(adapter->ctx);
  }
  tstr_freep(&adapter->host);
  tstr_freep(&adapter->username);
  tstr_freep(&adapter->password);
  tstr_freep(&adapter->from_name);
  tstr_freep(&adapter->from_email);
  tstr_freep(&adapter->to_name);
  tstr_freep(&adapter->to_email);
  tstr_freep(&adapter->subject);
  if (adapter->request_mutex_initialized) turbo_mutex_destroy(&adapter->request_mutex);
  adapter->ctx = NULL;
  free(adapter);
}

int turbo_flow_email_register_smtp_sink_adapter(
    turbo_flow_t *flow,
    const char *name,
    const turbo_flow_email_smtp_config_t *config) {
  flow_email_smtp_adapter_t *adapter;
  turbo_flow_adapter_ops_t ops;
  int rc;

  if (!flow || !name || name[0] == '\0') return TURBO_EINVAL;
  if (config &&
      (config->port < 0 || config->port > 65535 || config->timeout_ms < 0 ||
       (config->use_tls && config->use_starttls) || config->auth_method < SMTP_AUTH_NONE ||
       config->auth_method > SMTP_AUTH_CRAM_MD5)) {
    return TURBO_EINVAL;
  }

  adapter = (flow_email_smtp_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->quiesced, 0);
  if (tf_connection_init(&adapter->connection, "", 1u) != TURBO_OK) {
    free(adapter);
    return TURBO_ENOMEM;
  }
  turbo_mutex_init(&adapter->request_mutex);
  adapter->request_mutex_initialized = 1;

  if (config && config->context) {
    adapter->ctx = config->context;
    adapter->owns_context = config->take_context_ownership ? 1 : 0;
  } else {
    adapter->ctx = coro_context_create(NULL);
    adapter->owns_context = 1;
  }
  if (!adapter->ctx) {
    flow_email_smtp_shutdown(adapter);
    return TURBO_ENOMEM;
  }

  if (config) {
    char endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
    const char *scheme = config->use_tls ? "smtps" : "smtp";
    int written = snprintf(endpoint, sizeof(endpoint), "%s://%s:%d", scheme,
                           config->host ? config->host : "", config->port);
    if (written < 0 || (size_t)written >= sizeof(endpoint) ||
        tf_connection_set_endpoint(&adapter->connection, endpoint) != TURBO_OK) {
      flow_email_smtp_shutdown(adapter);
      return TURBO_ENOSPC;
    }
  }

  if (config) {
    adapter->port = config->port;
    adapter->use_tls = config->use_tls ? 1 : 0;
    adapter->use_starttls = config->use_starttls ? 1 : 0;
    adapter->auth_method = (smtp_auth_method_t)config->auth_method;
    adapter->timeout_ms = config->timeout_ms;
    adapter->html_body = config->html_body ? 1 : 0;
    adapter->max_pump_iterations = config->max_pump_iterations;

    rc = flow_email_dup_opt(&adapter->host, config->host);
    if (rc == TURBO_OK) rc = flow_email_dup_opt(&adapter->username, config->username);
    if (rc == TURBO_OK) rc = flow_email_dup_opt(&adapter->password, config->password);
    if (rc == TURBO_OK) rc = flow_email_dup_opt(&adapter->from_name, config->from_name);
    if (rc == TURBO_OK) rc = flow_email_dup_opt(&adapter->from_email, config->from_email);
    if (rc == TURBO_OK) rc = flow_email_dup_opt(&adapter->to_name, config->to_name);
    if (rc == TURBO_OK) rc = flow_email_dup_opt(&adapter->to_email, config->to_email);
    if (rc == TURBO_OK) rc = flow_email_dup_opt(&adapter->subject, config->subject);
    if (rc != TURBO_OK) {
      flow_email_smtp_shutdown(adapter);
      return rc;
    }
  }

  memset(&ops, 0, sizeof(ops));
  ops.start = flow_email_smtp_start;
  ops.consume = flow_email_smtp_consume;
  ops.stop = flow_email_smtp_stop;
  ops.shutdown = flow_email_smtp_shutdown;
  ops.connection_snapshot = flow_email_smtp_connection_snapshot;
  ops.command = flow_email_smtp_command;

  rc = turbo_flow_register_adapter_ex(flow, name, &ops, adapter, &FLOW_EMAIL_SMTP_SCHEMA);
  if (rc != TURBO_OK) {
    flow_email_smtp_shutdown(adapter);
    return rc;
  }
  return TURBO_OK;
}
