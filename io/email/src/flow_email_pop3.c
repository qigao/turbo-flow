#include "turbo_flow_email.h"

#include "email/email_pop3.h"
#include "flow_connection.h"
#include "flow_timer.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const turbo_flow_option_field_t FLOW_EMAIL_POP3_OPTION_FIELDS[] = {
    {"host", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"port", TURBO_FLOW_OPTION_U32,
     TURBO_FLOW_OPTION_REQUIRED | TURBO_FLOW_OPTION_HAS_MIN | TURBO_FLOW_OPTION_HAS_MAX,
     1, 65535, NULL, 0},
    {"use_tls", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"use_stls", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"username", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"password", TURBO_FLOW_OPTION_SECRET, TURBO_FLOW_OPTION_SECRET_VALUE, 0, 0, NULL, 0},
    {"timeout_ms", TURBO_FLOW_OPTION_DURATION_MS, 0, 0, 0, NULL, 0},
    {"poll_interval_ms", TURBO_FLOW_OPTION_DURATION_MS, TURBO_FLOW_OPTION_REQUIRED, 1,
     UINT32_MAX, NULL, 0}};

static const turbo_flow_adapter_schema_t FLOW_EMAIL_POP3_SCHEMA = {
    NULL, TURBO_FLOW_ADAPTER_KIND_EMAIL, TURBO_FLOW_ADAPTER_SOURCE,
    TURBO_FLOW_ADAPTER_INPUT, FLOW_EMAIL_POP3_OPTION_FIELDS,
    sizeof(FLOW_EMAIL_POP3_OPTION_FIELDS) / sizeof(FLOW_EMAIL_POP3_OPTION_FIELDS[0])};

typedef struct flow_email_pop3_adapter_s {
  tstr host;
  tstr username;
  tstr password;
  tstr source_name;
  tstr last_uidl;
  int port;
  int use_tls;
  int use_stls;
  int timeout_ms;
  uint32_t poll_interval_ms;
  turbo_flow_t *flow;
  turbo_thread_t thread;
  int thread_started;
  atomic_int started;
  atomic_int quiesced;
  tf_connection_state_t connection;
  tf_timer_t poll_wait;
  int poll_wait_initialized;
  turbo_mutex_t client_mutex;
  int client_mutex_initialized;
  pop3_client_t *active_client;
} flow_email_pop3_adapter_t;

typedef struct flow_email_pop3_poll_s {
  flow_email_pop3_adapter_t *adapter;
  coro_context_t *context;
  atomic_int done;
  int status;
  char *raw;
  size_t raw_len;
  tstr uidl;
} flow_email_pop3_poll_t;

static int flow_email_pop3_dup(tstr *out, const char *value) {
  if (!out) return TURBO_EINVAL;
  if (!value) return TURBO_OK;
  *out = tstr_dup(value);
  return *out ? TURBO_OK : TURBO_ENOMEM;
}

static void flow_email_pop3_free_uidls(char **uidls, int count) {
  if (!uidls) return;
  for (int i = 0; i < count; ++i) free(uidls[i]);
  free(uidls);
}

static void flow_email_pop3_poll_coro(coro_t *co, void *arg) {
  flow_email_pop3_poll_t *poll = (flow_email_pop3_poll_t *)arg;
  flow_email_pop3_adapter_t *adapter = poll ? poll->adapter : NULL;
  pop3_config_t config = {0};
  pop3_client_t *client = NULL;
  char **uidls = NULL;
  int count = 0;
  int status = TURBO_EIO;
  (void)co;
  if (!poll || !adapter) return;

  config.host = adapter->host;
  config.port = adapter->port;
  config.use_tls = adapter->use_tls;
  config.use_stls = adapter->use_stls;
  config.username = adapter->username;
  config.password = adapter->password;
  config.timeout_ms = adapter->timeout_ms;
  client = pop3_client_create(poll->context, &config);
  if (!client) {
    status = TURBO_ENOMEM;
    goto complete;
  }
  turbo_mutex_lock(&adapter->client_mutex);
  adapter->active_client = client;
  turbo_mutex_unlock(&adapter->client_mutex);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CONNECTING, TURBO_OK);
  if (pop3_connect(client) != 0) goto complete;
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_READY, TURBO_OK);
  tf_connection_set_usage(&adapter->connection, 1u, 0u, 0u);
  count = pop3_stat(client, NULL);
  if (count < 0) goto complete;
  if (count == 0) {
    status = TURBO_OK;
    goto complete;
  }
  uidls = pop3_uidl(client, &count);
  if (!uidls || count <= 0 || !uidls[count - 1]) goto complete;
  poll->uidl = tstr_dup(uidls[count - 1]);
  if (!poll->uidl) {
    status = TURBO_ENOMEM;
    goto complete;
  }
  if (adapter->last_uidl && strcmp(adapter->last_uidl, poll->uidl) == 0) {
    status = TURBO_OK;
    goto complete;
  }
  status = pop3_retrieve_raw(client, count, &poll->raw, &poll->raw_len);

complete:
  flow_email_pop3_free_uidls(uidls, count);
  turbo_mutex_lock(&adapter->client_mutex);
  if (adapter->active_client == client) adapter->active_client = NULL;
  turbo_mutex_unlock(&adapter->client_mutex);
  if (client) pop3_client_free(client);
  tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
  poll->status = status;
  atomic_store_explicit(&adapter->connection.last_status, status, memory_order_relaxed);
  atomic_store_explicit(&poll->done, 1, memory_order_release);
}

static int flow_email_pop3_poll(flow_email_pop3_adapter_t *adapter, coro_context_t *context) {
  flow_email_pop3_poll_t poll;
  turbo_flow_msg_t msg;
  int rc;
  memset(&poll, 0, sizeof(poll));
  poll.adapter = adapter;
  poll.context = context;
  poll.status = TURBO_EIO;
  atomic_init(&poll.done, 0);
  rc = coro_context_spawn(context, flow_email_pop3_poll_coro, &poll);
  if (rc != TURBO_OK) return rc;
  while (!atomic_load_explicit(&poll.done, memory_order_acquire)) {
    (void)coro_context_run(context, TURBO_RUN_ONCE);
  }
  rc = poll.status;
  if (rc == TURBO_OK && poll.raw) {
    turbo_flow_msg_init(&msg);
    msg.owned_payload = tstr_new_len(poll.raw, poll.raw_len);
    if (!msg.owned_payload) {
      rc = TURBO_ENOMEM;
    } else {
      msg.payload = tstr_to_v(msg.owned_payload);
      tf_connection_set_usage(&adapter->connection, 1u, 1u, poll.raw_len);
      rc = turbo_flow_publish(adapter->flow, adapter->source_name, &msg);
      tf_connection_set_usage(&adapter->connection, 1u, 0u, 0u);
      turbo_flow_msg_cleanup(&msg);
      if (rc == TURBO_OK) {
        tstr_freep(&adapter->last_uidl);
        adapter->last_uidl = poll.uidl;
        poll.uidl = NULL;
      }
    }
  }
  free(poll.raw);
  tstr_freep(&poll.uidl);
  return rc;
}

static void flow_email_pop3_source_thread(void *arg) {
  flow_email_pop3_adapter_t *adapter = (flow_email_pop3_adapter_t *)arg;
  coro_context_t *context = coro_context_create(NULL);
  if (!context) {
    tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, TURBO_ENOMEM);
    return;
  }
  while (atomic_load_explicit(&adapter->started, memory_order_acquire) && adapter->flow &&
         turbo_flow_state(adapter->flow) != TURBO_FLOW_STATE_STARTED) {
    turbo_sleep_ms(1);
  }
  while (atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    int rc;
    if (atomic_load_explicit(&adapter->quiesced, memory_order_acquire)) {
      rc = tf_timer_wait_for_ms(&adapter->poll_wait, adapter->poll_interval_ms);
      if (rc == TURBO_ESHUTDOWN) break;
      continue;
    }
    rc = flow_email_pop3_poll(adapter, context);
    if (!atomic_load_explicit(&adapter->started, memory_order_acquire)) break;
    if (rc != TURBO_OK) {
      tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_FAILED, rc);
    }
    rc = tf_timer_wait_for_ms(&adapter->poll_wait, adapter->poll_interval_ms);
    if (rc == TURBO_ESHUTDOWN) break;
  }
  coro_context_destroy(context);
}

static int flow_email_pop3_start(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flow_email_pop3_adapter_t *adapter = (flow_email_pop3_adapter_t *)ctx;
  if (!adapter || !flow || !stage || !stage->is_source) return TURBO_EINVAL;
  adapter->flow = flow;
  tstr_freep(&adapter->source_name);
  adapter->source_name = tstr_dup(stage->name);
  if (!adapter->source_name) return TURBO_ENOMEM;
  atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
  atomic_store_explicit(&adapter->started, 1, memory_order_release);
  tf_timer_reset(&adapter->poll_wait);
  if (turbo_thread_create(&adapter->thread, flow_email_pop3_source_thread, adapter) != TURBO_OK) {
    atomic_store_explicit(&adapter->started, 0, memory_order_release);
    return TURBO_EINVAL;
  }
  adapter->thread_started = 1;
  return TURBO_OK;
}

static void flow_email_pop3_interrupt(flow_email_pop3_adapter_t *adapter) {
  if (!adapter || !adapter->client_mutex_initialized) return;
  turbo_mutex_lock(&adapter->client_mutex);
  if (adapter->active_client) (void)pop3_interrupt(adapter->active_client, TURBO_ESHUTDOWN);
  turbo_mutex_unlock(&adapter->client_mutex);
}

static void flow_email_pop3_stop(void *ctx, turbo_flow_t *flow,
                                 const turbo_flow_stage_plan_t *stage) {
  flow_email_pop3_adapter_t *adapter = (flow_email_pop3_adapter_t *)ctx;
  (void)flow;
  (void)stage;
  if (!adapter) return;
  atomic_store_explicit(&adapter->started, 0, memory_order_release);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_CLOSING, TURBO_OK);
  if (adapter->poll_wait_initialized) tf_timer_stop(&adapter->poll_wait);
  flow_email_pop3_interrupt(adapter);
  if (adapter->thread_started) {
    (void)turbo_thread_join(&adapter->thread);
    adapter->thread_started = 0;
  }
  tf_connection_set_usage(&adapter->connection, 0u, 0u, 0u);
  tf_connection_transition(&adapter->connection, TURBO_FLOW_CONNECTION_STOPPED,
                           TURBO_ESHUTDOWN);
}

static int flow_email_pop3_snapshot(void *ctx, turbo_flow_connection_snapshot_t *out) {
  flow_email_pop3_adapter_t *adapter = (flow_email_pop3_adapter_t *)ctx;
  return adapter ? tf_connection_snapshot(&adapter->connection, out) : TURBO_EINVAL;
}

static int flow_email_pop3_command(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_adapter_command_t *command) {
  flow_email_pop3_adapter_t *adapter = (flow_email_pop3_adapter_t *)ctx;
  (void)flow;
  if (!adapter || !command || !atomic_load_explicit(&adapter->started, memory_order_acquire)) {
    return TURBO_EINVAL;
  }
  switch (command->kind) {
  case TURBO_FLOW_ADAPTER_QUIESCE:
    atomic_store_explicit(&adapter->quiesced, 1, memory_order_release);
    flow_email_pop3_interrupt(adapter);
    if (adapter->poll_wait_initialized) tf_timer_signal(&adapter->poll_wait);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_RESUME:
    atomic_store_explicit(&adapter->quiesced, 0, memory_order_release);
    if (adapter->poll_wait_initialized) tf_timer_signal(&adapter->poll_wait);
    return TURBO_OK;
  case TURBO_FLOW_ADAPTER_REPLACE_ENDPOINT:
    return TURBO_ENOTSUP;
  default:
    return TURBO_EINVAL;
  }
}

static void flow_email_pop3_shutdown(void *ctx) {
  flow_email_pop3_adapter_t *adapter = (flow_email_pop3_adapter_t *)ctx;
  if (!adapter) return;
  flow_email_pop3_stop(adapter, NULL, NULL);
  if (adapter->poll_wait_initialized) tf_timer_destroy(&adapter->poll_wait);
  if (adapter->client_mutex_initialized) turbo_mutex_destroy(&adapter->client_mutex);
  tstr_freep(&adapter->host);
  tstr_freep(&adapter->username);
  tstr_freep(&adapter->password);
  tstr_freep(&adapter->source_name);
  tstr_freep(&adapter->last_uidl);
  free(adapter);
}

int turbo_flow_email_register_pop3_source_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_email_pop3_config_t *config) {
  flow_email_pop3_adapter_t *adapter;
  turbo_flow_adapter_ops_t ops;
  char endpoint[TURBO_FLOW_ENDPOINT_MAX + 1u];
  int written;
  int rc;
  if (!flow || !name || !*name || !config || !config->host || !*config->host || config->port <= 0 ||
      config->port > 65535 || config->timeout_ms < 0 || (config->use_tls && config->use_stls) ||
      config->poll_interval_ms == 0) {
    return TURBO_EINVAL;
  }
  adapter = (flow_email_pop3_adapter_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  atomic_init(&adapter->started, 0);
  atomic_init(&adapter->quiesced, 0);
  if (tf_connection_init(&adapter->connection, "", 1u) != TURBO_OK) {
    free(adapter);
    return TURBO_ENOMEM;
  }
  adapter->port = config->port;
  adapter->use_tls = config->use_tls ? 1 : 0;
  adapter->use_stls = config->use_stls ? 1 : 0;
  adapter->timeout_ms = config->timeout_ms;
  adapter->poll_interval_ms = config->poll_interval_ms;
  rc = flow_email_pop3_dup(&adapter->host, config->host);
  if (rc == TURBO_OK) rc = flow_email_pop3_dup(&adapter->username, config->username);
  if (rc == TURBO_OK) rc = flow_email_pop3_dup(&adapter->password, config->password);
  if (rc != TURBO_OK) {
    flow_email_pop3_shutdown(adapter);
    return rc;
  }
  written = snprintf(endpoint, sizeof(endpoint), "%s://%s:%d",
                     config->use_tls ? "pop3s" : "pop3", config->host, config->port);
  if (written < 0 || (size_t)written >= sizeof(endpoint) ||
      tf_connection_set_endpoint(&adapter->connection, endpoint) != TURBO_OK) {
    flow_email_pop3_shutdown(adapter);
    return TURBO_ENOSPC;
  }
  turbo_mutex_init(&adapter->client_mutex);
  adapter->client_mutex_initialized = 1;
  if (tf_timer_init(&adapter->poll_wait) != TURBO_OK) {
    flow_email_pop3_shutdown(adapter);
    return TURBO_ENOMEM;
  }
  adapter->poll_wait_initialized = 1;
  memset(&ops, 0, sizeof(ops));
  ops.start = flow_email_pop3_start;
  ops.stop = flow_email_pop3_stop;
  ops.shutdown = flow_email_pop3_shutdown;
  ops.connection_snapshot = flow_email_pop3_snapshot;
  ops.command = flow_email_pop3_command;
  rc = turbo_flow_register_adapter_ex(flow, name, &ops, adapter, &FLOW_EMAIL_POP3_SCHEMA);
  if (rc != TURBO_OK) flow_email_pop3_shutdown(adapter);
  return rc;
}
