#include "turbo_flow_cnet.h"

#include <cflow/scheduler.h>
#include <salts_error.h>
#include <tstr.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct turbo_flow_cnet_stream_source_s {
  turbo_flow_cnet_stream_source_state_t state;
  int status;
  int native_status;
  char error_stage[TURBO_FLOW_CNET_STREAM_SOURCE_ERROR_STAGE_CAPACITY];
  char publisher_error[192];
  tstr source_name;
  cnet_client client;
  cnet_connection connection;
  cflow_scheduler scheduler;
  turbo_flow_run_t *run;
  turbo_flow_msg_t ready_message;
  cflow_waker value_waker;
  cflow_waker terminal_waker;
  turbo_flow_content_descriptor_t content;
  size_t max_message_bytes;
  size_t scheduler_max_steps_per_poll;
  uint64_t next_message_id;
  uint64_t messages_received;
  uint64_t bytes_received;
  bool client_initialized;
  bool scheduler_initialized;
  bool ready;
  bool receive_pending;
  bool publisher_destroyed;
  bool publisher_cancelled;
  bool has_content;
  bool message_id_exhausted;
};

static bool stream_source_connection_equal(cnet_connection lhs, cnet_connection rhs) {
  return lhs.slot == rhs.slot && lhs.generation == rhs.generation;
}

static void stream_source_wake(cflow_waker *waker) {
  cflow_waker pending;
  if (!waker || !waker->wake) return;
  pending = *waker;
  *waker = (cflow_waker){0};
  pending.wake(pending.user);
}

static void stream_source_copy_stage(char *destination, size_t capacity, const char *stage) {
  if (!destination || capacity == 0u) return;
  if (!stage) stage = "cnet";
  (void)snprintf(destination, capacity, "%s", stage);
}

static void stream_source_fail(turbo_flow_cnet_stream_source_t *source, int status,
                               int native_status, const char *stage) {
  if (!source || source->state == TURBO_FLOW_CNET_STREAM_SOURCE_FAILED ||
      source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED) {
    return;
  }
  source->state = TURBO_FLOW_CNET_STREAM_SOURCE_FAILED;
  source->status = status == SALTS_OK ? SALTS_EIO : status;
  source->native_status = native_status;
  source->receive_pending = false;
  stream_source_copy_stage(source->error_stage, sizeof(source->error_stage), stage);
  (void)snprintf(source->publisher_error, sizeof(source->publisher_error),
                 "CNet stream source failed at %s: %s (%d, native=%d)", source->error_stage,
                 salts_strerror(source->status), source->status, source->native_status);
  if (source->client_initialized && source->connection.generation != 0u) {
    (void)cnet_close(&source->client, source->connection);
  }
  stream_source_wake(&source->value_waker);
  stream_source_wake(&source->terminal_waker);
}

static bool stream_source_waitable_arm(void *state, cflow_waker waker) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)state;
  if (!source || source->publisher_destroyed) return false;
  source->value_waker = waker;
  return true;
}

static void stream_source_waitable_cancel(void *state) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)state;
  if (source) source->value_waker = (cflow_waker){0};
}

CMETA_IMPLEMENTS(cflow_waitable, stream_source_waitable, 0, .arm = stream_source_waitable_arm,
                 .cancel = stream_source_waitable_cancel);

static const char *stream_source_publisher_name(void *state) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)state;
  return source && source->source_name ? source->source_name : "cnet-stream-source";
}

static const cmeta_type_desc *stream_source_publisher_type(void *state) {
  (void)state;
  return turbo_flow_message_type();
}

static cflow_step stream_source_publisher_resume(void *state, cflow_publish_context *context,
                                                 void *out_value) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)state;
  int rc;
  (void)context;
  if (!source || !out_value) {
    return (cflow_step){CFLOW_STEP_ERROR, {0}, "invalid CNet stream source"};
  }
  if (source->state == TURBO_FLOW_CNET_STREAM_SOURCE_FAILED) {
    return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
  }
  if (source->publisher_cancelled || source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED ||
      source->state == TURBO_FLOW_CNET_STREAM_SOURCE_REMOTE_CLOSED) {
    return (cflow_step){CFLOW_STEP_DONE, {0}, NULL};
  }
  if (source->ready) {
    rc = turbo_flow_msg_move((turbo_flow_msg_t *)out_value, &source->ready_message);
    if (rc != SALTS_OK) {
      stream_source_fail(source, rc, 0, "message_move");
      return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
    }
    source->ready = false;
    return (cflow_step){CFLOW_STEP_VALUE, {0}, NULL};
  }
  if (source->message_id_exhausted) {
    stream_source_fail(source, SALTS_ERANGE, 0, "message_id");
    return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
  }
  if (source->state == TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTED && !source->receive_pending) {
    rc = cnet_receive(&source->client, source->connection, 1u);
    if (rc != SALTS_OK) {
      stream_source_fail(source, rc, 0, "receive_admission");
      return (cflow_step){CFLOW_STEP_ERROR, {0}, source->publisher_error};
    }
    source->receive_pending = true;
  }
  return (cflow_step){CFLOW_STEP_WAIT, stream_source_waitable_as_cflow_waitable(source), NULL};
}

static void stream_source_publisher_cancel(void *state) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)state;
  if (!source) return;
  source->publisher_cancelled = true;
  source->value_waker = (cflow_waker){0};
  source->terminal_waker = (cflow_waker){0};
}

static void stream_source_publisher_destroy(void *state) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)state;
  if (!source) return;
  source->publisher_destroyed = true;
  source->value_waker = (cflow_waker){0};
  source->terminal_waker = (cflow_waker){0};
}

static void stream_source_publisher_bind_terminal(void *state, cflow_waker waker) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)state;
  if (source && !source->publisher_destroyed) source->terminal_waker = waker;
}

static cflow_publisher_terminal stream_source_publisher_poll_terminal(void *state,
                                                                      const char **error) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)state;
  if (error) *error = NULL;
  if (!source) {
    if (error) *error = "invalid CNet stream source";
    return CFLOW_PUBLISHER_ERROR;
  }
  if (source->state == TURBO_FLOW_CNET_STREAM_SOURCE_FAILED) {
    if (error) *error = source->publisher_error;
    return CFLOW_PUBLISHER_ERROR;
  }
  if (source->publisher_cancelled || source->state == TURBO_FLOW_CNET_STREAM_SOURCE_REMOTE_CLOSED ||
      source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED) {
    return CFLOW_PUBLISHER_DONE;
  }
  return CFLOW_PUBLISHER_OPEN;
}

CMETA_IMPLEMENTS(cflow_publisher, stream_source_publisher, CFLOW_PUBLISHER_CAP_CONSTRUCTS_VALUES,
                 .name = stream_source_publisher_name, .output_type = stream_source_publisher_type,
                 .resume = stream_source_publisher_resume, .cancel = stream_source_publisher_cancel,
                 .destroy = stream_source_publisher_destroy,
                 .bind_terminal_waker = stream_source_publisher_bind_terminal,
                 .poll_terminal = stream_source_publisher_poll_terminal);

static void stream_source_on_state(void *user, cnet_connection connection,
                                   cnet_connection_state state, const cnet_error *error) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)user;
  if (!source || !stream_source_connection_equal(connection, source->connection)) return;
  switch (state) {
  case CNET_CONNECTION_CONNECTED:
    if (source->state == TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTING) {
      source->state = TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTED;
      stream_source_wake(&source->value_waker);
    }
    break;
  case CNET_CONNECTION_CLOSED:
    source->receive_pending = false;
    if (source->state != TURBO_FLOW_CNET_STREAM_SOURCE_STOPPING &&
        source->state != TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED &&
        source->state != TURBO_FLOW_CNET_STREAM_SOURCE_FAILED) {
      source->state = TURBO_FLOW_CNET_STREAM_SOURCE_REMOTE_CLOSED;
      source->status = SALTS_OK;
      stream_source_wake(&source->value_waker);
      stream_source_wake(&source->terminal_waker);
    }
    break;
  case CNET_CONNECTION_FAILED:
    stream_source_fail(source, error ? error->status : SALTS_EIO, error ? error->native_status : 0,
                       error ? error->stage : "connection");
    break;
  case CNET_CONNECTION_CONNECTING:
  case CNET_CONNECTION_CLOSING:
    break;
  }
}

static void stream_source_on_receive(void *user, cnet_connection connection,
                                     const cnet_receive_view *view) {
  turbo_flow_cnet_stream_source_t *source = (turbo_flow_cnet_stream_source_t *)user;
  mem_buffer_t *buffer;
  int rc;
  if (!source || !stream_source_connection_equal(connection, source->connection)) return;
  source->receive_pending = false;
  if (!view || view->kind != CNET_MESSAGE_BYTES || !view->data || view->size == 0u) {
    stream_source_fail(source, SALTS_EPROTO, 0, "receive_view");
    return;
  }
  if (source->ready) {
    stream_source_fail(source, SALTS_EBUSY, 0, "receive_ownership");
    return;
  }
  if (view->size > source->max_message_bytes) {
    stream_source_fail(source, SALTS_EMSGSIZE, 0, "receive_size");
    return;
  }
  if (source->bytes_received > UINT64_MAX - (uint64_t)view->size ||
      source->messages_received == UINT64_MAX) {
    stream_source_fail(source, SALTS_ERANGE, 0, "receive_counters");
    return;
  }
  buffer = mem_get_buffer(mem_global(), view->size);
  if (!buffer) {
    stream_source_fail(source, SALTS_ENOMEM, 0, "receive_copy");
    return;
  }
  memcpy(mem_buffer_data(buffer), view->data, view->size);
  mem_set_used(buffer, view->size);
  turbo_flow_msg_init(&source->ready_message);
  source->ready_message.id = source->next_message_id;
  source->ready_message.buffer = buffer;
  source->ready_message.payload = vstr_from_buf(mem_buffer_const_data(buffer), view->size);
  if (source->has_content) {
    rc = turbo_flow_msg_copy_content_descriptor(&source->ready_message, &source->content);
    if (rc != SALTS_OK) {
      turbo_flow_msg_cleanup(&source->ready_message);
      stream_source_fail(source, rc, 0, "content_descriptor");
      return;
    }
  }
  source->message_id_exhausted = source->next_message_id == UINT64_MAX;
  if (!source->message_id_exhausted) ++source->next_message_id;
  ++source->messages_received;
  source->bytes_received += (uint64_t)view->size;
  source->ready = true;
  stream_source_wake(&source->value_waker);
}

static int stream_source_config_validate(const turbo_flow_cnet_stream_source_config_t *config) {
  bool is_tcp;
  bool is_tls;
  bool is_pipe;
  int rc;
  if (!config || config->size < sizeof(*config) ||
      config->version != TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION || !config->flow ||
      !config->source_name || config->source_name[0] == '\0' || !config->uri ||
      config->uri[0] == '\0' || !config->client || config->max_message_bytes == 0u ||
      config->scheduler_capacity == 0u || config->scheduler_max_steps_per_poll == 0u ||
      config->first_message_id == 0u ||
      config->max_message_bytes > config->client->receive_buffer_bytes) {
    return SALTS_EINVAL;
  }
  if (turbo_flow_state(config->flow) != TURBO_FLOW_STATE_STARTED) return SALTS_EBUSY;
  is_tcp = strncmp(config->uri, "tcp://", sizeof("tcp://") - 1u) == 0;
  is_tls = strncmp(config->uri, "tls://", sizeof("tls://") - 1u) == 0;
  is_pipe = strncmp(config->uri, "pipe://", sizeof("pipe://") - 1u) == 0;
  if (!is_tcp && !is_tls && !is_pipe) return SALTS_ENOTSUP;
  if (config->tls && !is_tls) return SALTS_EINVAL;
  if (config->socket_options) {
    rc = cnet_stream_socket_options_validate(config->socket_options);
    if (rc != SALTS_OK) return rc;
  }
  if (config->content) {
    rc = turbo_flow_content_descriptor_check(config->content);
    if (rc != SALTS_OK) return rc;
  }
  return SALTS_OK;
}

static void stream_source_open_cleanup(turbo_flow_cnet_stream_source_t *source,
                                       cflow_publisher *publisher) {
  if (!source) return;
  if (source->run) {
    turbo_flow_run_close(source->run);
    source->run = NULL;
  } else if (publisher && cflow_publisher_valid(publisher)) {
    cflow_publisher_destroy(publisher);
  }
  if (source->client_initialized) {
    (void)cnet_client_stop(&source->client, 0u);
    (void)cnet_client_destroy(&source->client);
  }
  if (source->scheduler_initialized) cflow_scheduler_destroy(&source->scheduler);
  turbo_flow_msg_cleanup(&source->ready_message);
  tstr_free(source->source_name);
  free(source);
}

int turbo_flow_cnet_stream_source_open(const turbo_flow_cnet_stream_source_config_t *config,
                                       turbo_flow_cnet_stream_source_t **source_out) {
  turbo_flow_cnet_stream_source_t *source;
  turbo_flow_run_config_t run_config = TURBO_FLOW_RUN_CONFIG_INIT;
  cnet_connect_options connect_options = {0};
  cflow_publisher publisher = {0};
  int rc;
  if (!source_out) return SALTS_EINVAL;
  *source_out = NULL;
  rc = stream_source_config_validate(config);
  if (rc != SALTS_OK) return rc;
  source = (turbo_flow_cnet_stream_source_t *)calloc(1u, sizeof(*source));
  if (!source) return SALTS_ENOMEM;
  source->state = TURBO_FLOW_CNET_STREAM_SOURCE_NEW;
  source->status = SALTS_OK;
  source->source_name = tstr_dup(config->source_name);
  source->max_message_bytes = config->max_message_bytes;
  source->scheduler_max_steps_per_poll = config->scheduler_max_steps_per_poll;
  source->next_message_id = config->first_message_id;
  turbo_flow_msg_init(&source->ready_message);
  if (!source->source_name) {
    stream_source_open_cleanup(source, &publisher);
    return SALTS_ENOMEM;
  }
  if (config->content) {
    source->content = *config->content;
    source->has_content = true;
  }
  if (!cflow_scheduler_manual_init_with_capacity(&source->scheduler, config->scheduler_capacity)) {
    stream_source_open_cleanup(source, &publisher);
    return SALTS_ENOMEM;
  }
  source->scheduler_initialized = true;
  rc = cnet_client_init(&source->client, config->client);
  if (rc != SALTS_OK) {
    stream_source_open_cleanup(source, &publisher);
    return rc;
  }
  source->client_initialized = true;
  if (config->socket_options) {
    rc = cnet_client_set_stream_socket_options(&source->client, config->socket_options);
    if (rc != SALTS_OK) {
      stream_source_open_cleanup(source, &publisher);
      return rc;
    }
  }
  publisher = stream_source_publisher_as_cflow_publisher(source);
  run_config.scheduler = &source->scheduler;
  rc =
      turbo_flow_run_open(config->flow, config->source_name, &publisher, &run_config, &source->run);
  if (rc != SALTS_OK) {
    stream_source_open_cleanup(source, &publisher);
    return rc;
  }
  connect_options.uri = config->uri;
  connect_options.tls = config->tls;
  connect_options.observer.on_state = stream_source_on_state;
  connect_options.observer.on_receive = stream_source_on_receive;
  connect_options.observer.user = source;
  source->state = TURBO_FLOW_CNET_STREAM_SOURCE_CONNECTING;
  rc = cnet_connect(&source->client, &connect_options, &source->connection);
  if (rc != SALTS_OK) {
    stream_source_open_cleanup(source, &publisher);
    return rc;
  }
  *source_out = source;
  return SALTS_OK;
}

static void stream_source_refresh_run(turbo_flow_cnet_stream_source_t *source) {
  turbo_flow_run_result_t result = TURBO_FLOW_RUN_RESULT_INIT;
  if (!source || !source->run || source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED) {
    return;
  }
  if (turbo_flow_run_snapshot(source->run, &result) != SALTS_OK) return;
  if (result.state == TURBO_FLOW_RUN_FAILED || result.state == TURBO_FLOW_RUN_CANCELED) {
    stream_source_fail(source, result.status, 0, "graph_run");
  }
}

int turbo_flow_cnet_stream_source_request(turbo_flow_cnet_stream_source_t *source, size_t demand) {
  int rc;
  if (!source || demand == 0u) return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_CNET_STREAM_SOURCE_FAILED) return source->status;
  if (source->state == TURBO_FLOW_CNET_STREAM_SOURCE_REMOTE_CLOSED ||
      source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPING ||
      source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED || !source->run) {
    return SALTS_ESHUTDOWN;
  }
  rc = turbo_flow_run_request(source->run, demand);
  stream_source_refresh_run(source);
  return source->state == TURBO_FLOW_CNET_STREAM_SOURCE_FAILED ? source->status : rc;
}

int turbo_flow_cnet_stream_source_snapshot(const turbo_flow_cnet_stream_source_t *source,
                                           turbo_flow_cnet_stream_source_snapshot_t *snapshot) {
  turbo_flow_cnet_stream_source_snapshot_t current = TURBO_FLOW_CNET_STREAM_SOURCE_SNAPSHOT_INIT;
  turbo_flow_run_result_t run_result = TURBO_FLOW_RUN_RESULT_INIT;
  if (!source || !snapshot || snapshot->size < sizeof(*snapshot) ||
      snapshot->version != TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION) {
    return SALTS_EINVAL;
  }
  current.state = source->state;
  current.status = source->status;
  current.native_status = source->native_status;
  current.connection = source->connection;
  current.messages_received = source->messages_received;
  current.bytes_received = source->bytes_received;
  current.receive_pending = source->receive_pending ? 1 : 0;
  stream_source_copy_stage(current.error_stage, sizeof(current.error_stage),
                           source->error_stage[0] ? source->error_stage : "");
  if (source->run && turbo_flow_run_snapshot(source->run, &run_result) == SALTS_OK) {
    current.outstanding_demand = run_result.outstanding_demand;
  }
  *snapshot = current;
  return SALTS_OK;
}

int turbo_flow_cnet_stream_source_poll(turbo_flow_cnet_stream_source_t *source, uint32_t timeout_ms,
                                       turbo_flow_cnet_stream_source_snapshot_t *snapshot) {
  size_t events = 0u;
  int rc = SALTS_OK;
  if (!source || (snapshot && (snapshot->size < sizeof(*snapshot) ||
                               snapshot->version != TURBO_FLOW_CNET_STREAM_SOURCE_API_VERSION))) {
    return SALTS_EINVAL;
  }
  if (source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED) return SALTS_ESHUTDOWN;
  if (source->state != TURBO_FLOW_CNET_STREAM_SOURCE_FAILED) {
    (void)cflow_scheduler_run_until_idle(&source->scheduler, source->scheduler_max_steps_per_poll);
    stream_source_refresh_run(source);
  }
  if (source->state != TURBO_FLOW_CNET_STREAM_SOURCE_FAILED &&
      source->state != TURBO_FLOW_CNET_STREAM_SOURCE_STOPPING) {
    rc = cnet_client_poll(&source->client, timeout_ms, &events);
    if (rc != SALTS_OK) stream_source_fail(source, rc, 0, "client_poll");
  }
  if (source->state != TURBO_FLOW_CNET_STREAM_SOURCE_FAILED) {
    (void)cflow_scheduler_run_until_idle(&source->scheduler, source->scheduler_max_steps_per_poll);
    stream_source_refresh_run(source);
  }
  if (snapshot) (void)turbo_flow_cnet_stream_source_snapshot(source, snapshot);
  return source->state == TURBO_FLOW_CNET_STREAM_SOURCE_FAILED ? source->status : rc;
}

int turbo_flow_cnet_stream_source_stop(turbo_flow_cnet_stream_source_t *source,
                                       uint32_t timeout_ms) {
  int stop_rc;
  int destroy_rc;
  if (!source) return SALTS_EINVAL;
  if (source->state == TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED) return SALTS_EALREADY;
  source->state = TURBO_FLOW_CNET_STREAM_SOURCE_STOPPING;
  if (source->run) {
    turbo_flow_run_close(source->run);
    source->run = NULL;
  }
  stop_rc = source->client_initialized ? cnet_client_stop(&source->client, timeout_ms) : SALTS_OK;
  if (stop_rc == SALTS_ETIMEDOUT) return stop_rc;
  destroy_rc = source->client_initialized ? cnet_client_destroy(&source->client) : SALTS_OK;
  if (destroy_rc != SALTS_OK) return stop_rc == SALTS_OK ? destroy_rc : stop_rc;
  source->client_initialized = false;
  if (source->scheduler_initialized) {
    (void)cflow_scheduler_run_until_idle(&source->scheduler, 0u);
    (void)cflow_scheduler_shutdown(&source->scheduler);
    cflow_scheduler_destroy(&source->scheduler);
    source->scheduler_initialized = false;
  }
  if (source->ready) {
    turbo_flow_msg_cleanup(&source->ready_message);
    source->ready = false;
  }
  source->receive_pending = false;
  source->state = TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED;
  return stop_rc;
}

int turbo_flow_cnet_stream_source_destroy(turbo_flow_cnet_stream_source_t *source) {
  if (!source) return SALTS_EINVAL;
  if (source->state != TURBO_FLOW_CNET_STREAM_SOURCE_STOPPED) return SALTS_EBUSY;
  turbo_flow_msg_cleanup(&source->ready_message);
  tstr_free(source->source_name);
  free(source);
  return SALTS_OK;
}
